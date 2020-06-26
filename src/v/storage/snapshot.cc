#include "storage/snapshot.h"

#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "utils/directory_walker.h"

#include <regex>

namespace storage {

std::ostream& operator<<(std::ostream& os, const snapshot_metadata& meta) {
    fmt::print(
      os,
      "last_index {} last_term {}",
      meta.last_included_index(),
      meta.last_included_term());
    return os;
}

ss::future<std::optional<snapshot_reader>> snapshot_manager::open_snapshot() {
    auto path = _dir / snapshot_filename;
    return ss::file_exists(path.string()).then([this, path](bool exists) {
        if (!exists) {
            return ss::make_ready_future<std::optional<snapshot_reader>>(
              std::nullopt);
        }
        return ss::open_file_dma(path.string(), ss::open_flags::ro)
          .then([this, path](ss::file file) {
              // ss::file::~file will automatically close the file. so no
              // worries about leaking an fd if something goes wrong here.
              ss::file_input_stream_options options;
              options.io_priority_class = _io_prio;
              auto input = ss::make_file_input_stream(file, options);
              return ss::make_ready_future<std::optional<snapshot_reader>>(
                snapshot_reader(file, std::move(input), path));
          });
    });
}

ss::future<snapshot_writer> snapshot_manager::start_snapshot() {
    // the random suffix is added because the lowres clock doesn't produce
    // unique file names when tests run fast.
    auto filename = fmt::format(
      "{}.partial.{}.{}",
      snapshot_filename,
      ss::lowres_system_clock::now().time_since_epoch().count(),
      random_generators::gen_alphanum_string(4));

    auto path = _dir / filename;

    const auto flags = ss::open_flags::wo | ss::open_flags::create
                       | ss::open_flags::exclusive;

    return ss::open_file_dma(path.string(), flags)
      .then([this, path](ss::file file) {
          ss::file_output_stream_options options;
          options.io_priority_class = _io_prio;
          auto output = ss::make_file_output_stream(file, options);
          return snapshot_writer(std::move(output), path);
      });
}

ss::future<> snapshot_manager::finish_snapshot(snapshot_writer& writer) {
    return ss::rename_file(writer.path().string(), snapshot_path().string())
      .then([this] { return ss::sync_directory(_dir.string()); });
}

ss::future<> snapshot_manager::remove_partial_snapshots() {
    std::regex re(fmt::format(
      "^{}\\.partial\\.(\\d+)\\.([a-zA-Z0-9]{{4}})$", snapshot_filename));
    return directory_walker::walk(
      _dir.string(), [this, re = std::move(re)](ss::directory_entry ent) {
          if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
              return ss::now();
          }
          std::cmatch match;
          if (std::regex_match(ent.name.c_str(), match, re)) {
              auto path = _dir / ent.name.c_str();
              return ss::remove_file(path.string());
          }
          return ss::now();
      });
}

snapshot_metadata snapshot_reader::parse_metadata(iobuf buf) {
    iobuf_parser parser(std::move(buf));

    // integral types automatically converted from le
    auto last_included_index = model::offset(
      reflection::adl<model::offset::type>{}.from(parser));

    auto last_included_term = model::term_id(
      reflection::adl<model::term_id::type>{}.from(parser));

    vassert(
      parser.bytes_consumed() == snapshot_metadata::ondisk_size,
      "Error parsing snapshot metadata. Consumed {} bytes expected {}",
      parser.bytes_consumed(),
      snapshot_metadata::ondisk_size);

    return snapshot_metadata{
      .last_included_index = last_included_index,
      .last_included_term = last_included_term,
    };
}

ss::future<snapshot_header> snapshot_reader::read_header() {
    return read_iobuf_exactly(_input, snapshot_header::ondisk_size)
      .then([this](iobuf buf) {
          if (buf.size_bytes() != snapshot_header::ondisk_size) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Snapshot file does not contain full header: {}", _path)));
          }

          iobuf_parser parser(std::move(buf));
          snapshot_header hdr;
          hdr.header_crc = reflection::adl<uint32_t>{}.from(parser);
          hdr.metadata_crc = reflection::adl<uint32_t>{}.from(parser);
          hdr.version = reflection::adl<int8_t>{}.from(parser);
          hdr.size = reflection::adl<int32_t>{}.from(parser);

          vassert(
            parser.bytes_consumed() == snapshot_header::ondisk_size,
            "Error parsing snapshot header. Consumed {} bytes expected {}",
            parser.bytes_consumed(),
            snapshot_header::ondisk_size);

          vassert(hdr.size > 0, "Invalid metadata size {}", hdr.size);

          crc32 crc;
          crc.extend(ss::cpu_to_le(hdr.metadata_crc));
          crc.extend(ss::cpu_to_le(hdr.version));
          crc.extend(ss::cpu_to_le(hdr.size));

          if (hdr.header_crc != crc.value()) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Corrupt snapshot. Failed to verify header crc: {} != {}: {}",
                  crc.value(),
                  hdr.header_crc,
                  _path)));
          }

          if (hdr.version != snapshot_header::supported_version) {
              return ss::make_exception_future<snapshot_header>(
                std::runtime_error(fmt::format(
                  "Invalid snapshot version {} != {}: {}",
                  hdr.version,
                  snapshot_header::supported_version,
                  _path)));
          }

          return ss::make_ready_future<snapshot_header>(hdr);
      });
}

ss::future<snapshot_metadata> snapshot_reader::read_metadata() {
    return read_header().then([this](snapshot_header header) {
        return read_iobuf_exactly(_input, snapshot_metadata::ondisk_size)
          .then([this, header](iobuf buf) {
              if (buf.size_bytes() != snapshot_metadata::ondisk_size) {
                  return ss::make_exception_future<snapshot_metadata>(
                    std::runtime_error(fmt::format(
                      "Corrupt snapshot. Failed to read metadata: {}", _path)));
              }

              auto metadata = parse_metadata(std::move(buf));

              crc32 crc;
              crc.extend(ss::cpu_to_le(metadata.last_included_index()));
              crc.extend(ss::cpu_to_le(metadata.last_included_term()));

              if (header.metadata_crc != crc.value()) {
                  return ss::make_exception_future<snapshot_metadata>(
                    std::runtime_error(fmt::format(
                      "Corrupt snapshot. Failed to verify metadata crc: {} != "
                      "{}: {}",
                      crc.value(),
                      header.metadata_crc,
                      _path)));
              }

              return ss::make_ready_future<snapshot_metadata>(
                std::move(metadata));
          });
    });
}

ss::future<> snapshot_reader::close() {
    return _input
      .close() // finishes read-ahead work
      .then([this] { return _file.close(); });
}

ss::future<>
snapshot_writer::write_metadata(const snapshot_metadata& metadata) {
    snapshot_header header;
    header.version = snapshot_header::supported_version;

    // integral types auto serialize to le
    iobuf buf;
    reflection::serialize(
      buf, metadata.last_included_index, metadata.last_included_term);
    header.size = buf.size_bytes();

    vassert(
      buf.size_bytes() == snapshot_metadata::ondisk_size,
      "Unexpected snapshot metadata serialization size {} != {}",
      buf.size_bytes(),
      snapshot_metadata::ondisk_size);

    // crc computed over le bytes
    crc32 meta_crc;
    meta_crc.extend(ss::cpu_to_le(metadata.last_included_index()));
    meta_crc.extend(ss::cpu_to_le(metadata.last_included_term()));
    header.metadata_crc = meta_crc.value();

    crc32 header_crc;
    header_crc.extend(ss::cpu_to_le(header.metadata_crc));
    header_crc.extend(ss::cpu_to_le(header.version));
    header_crc.extend(ss::cpu_to_le(header.size));
    header.header_crc = header_crc.value();

    iobuf header_buf;
    reflection::serialize(
      header_buf,
      header.header_crc,
      header.metadata_crc,
      header.version,
      header.size);

    vassert(
      header_buf.size_bytes() == snapshot_header::ondisk_size,
      "Unexpected snapshot header serialization size {} != {}",
      header_buf.size_bytes(),
      snapshot_header::ondisk_size);

    buf.prepend(std::move(header_buf));
    return write_iobuf_to_output_stream(std::move(buf), _output);
}

ss::future<> snapshot_writer::close() {
    return _output.flush().then([this] { return _output.close(); });
}

} // namespace storage