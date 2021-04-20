/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf_parser.h"
#include "reflection/type_traits.h"
#include "serde/envelope_for_each_field.h"
#include "serde/error.h"
#include "serde/logger.h"
#include "serde/type_str.h"
#include "ssx/future-util.h"
#include "utils/named_type.h"

#include <iosfwd>
#include <numeric>
#include <string>
#include <string_view>

namespace serde {

#define SERDE_TOKEN_PASTE(x, y) x##y
#define SERDE_TOKEN_PASTE2(x, y) SERDE_TOKEN_PASTE(x, y)

#define HAS_MEMBER_FN(name, ...)                                               \
    template<typename T, typename = void>                                      \
    struct SERDE_TOKEN_PASTE(help_has_, name)                                  \
      : std::false_type {};                                                    \
                                                                               \
    template<typename T>                                                       \
    struct SERDE_TOKEN_PASTE(                                                  \
      help_has_,                                                               \
      name)<T, std::void_t<decltype(std::declval<T>().name(__VA_ARGS__))>>     \
      : std::true_type {};                                                     \
                                                                               \
    template<typename T>                                                       \
    inline constexpr auto const SERDE_TOKEN_PASTE(                             \
      has_, name) = SERDE_TOKEN_PASTE(help_has_, name)<T>::value;

static auto iob = iobuf{};
static auto iop = iobuf_parser{std::move(iob)};

HAS_MEMBER_FN(serde_read, iop, version_t{0}, version_t{0}, size_t{0U})
HAS_MEMBER_FN(serde_write, iob)
HAS_MEMBER_FN(serde_async_read, iop, version_t{0}, version_t{0}, size_t{0U})
HAS_MEMBER_FN(serde_async_write, iob)

template<typename T>
inline constexpr auto const is_serde_compatible_v =
  is_envelope_v<T> || std::is_scalar_v<T> || std::is_enum_v<T> ||
  reflection::is_std_vector_v<T> || reflection::is_named_type_v<T> ||
  reflection::is_ss_bool_v<T> || std::is_same_v<T, std::chrono::milliseconds> ||
  std::is_same_v<T, iobuf> || std::is_same_v<T, ss::sstring> ||
  reflection::is_std_optional_v<T>;

using header_t = std::tuple<version_t, version_t, size_t>;

void write_envelope() {}

template<typename T>
void write(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_write<Type> || is_serde_compatible_v<Type>);

    if constexpr (is_envelope_v<Type>) {
        write(out, Type::__version);
        write(out, Type::__compat_version);

        auto size_placeholder = out.reserve(
          sizeof(typename Type::envelope_size_t));
        auto const size_before = out.size_bytes();

        if constexpr (has_serde_write<Type>) {
            t.serde_write(out);
        } else {
            envelope_for_each_field(t, [&out](auto& f) { write(out, f); });
        }

        auto const written_size = out.size_bytes() - size_before;
        if (
          written_size >
          std::numeric_limits<typename Type::envelope_size_t>::max()) {
            throw std::system_error{
              make_error_code(error_codes::envelope_too_big)};
        }
        auto const size = ss::cpu_to_le(
          static_cast<typename Type::envelope_size_t>(written_size));
        size_placeholder.write(
          reinterpret_cast<char const*>(&size),
          sizeof(typename Type::envelope_size_t));
    } else if constexpr (std::is_enum_v<Type>) {
        write(out, static_cast<std::underlying_type_t<Type>>(t));
    } else if constexpr (std::is_scalar_v<Type>) {
        auto le_t = ss::cpu_to_le(t);
        out.append(reinterpret_cast<char const*>(&le_t), sizeof(Type));
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        assert(t.size() <= std::numeric_limits<int32_t>::max());
        write(out, static_cast<int32_t>(t.size()));
        for (auto const& el : t) {
            write(out, el);
        }
    } else if constexpr (reflection::is_named_type_v<Type>) {
        return write(iob, static_cast<typename Type::type>(t));
    } else if constexpr (reflection::is_ss_bool_v<Type>) {
        write(iob, static_cast<int8_t>(bool(t)));
    } else if constexpr (std::is_same_v<Type, std::chrono::milliseconds>) {
        write<int64_t>(iob, t.count());
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        write<int32_t>(out, t.size_bytes());
        out.append(std::move(t));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        write<int32_t>(out, t.size());
        out.append(t.data(), t.size());
    } else if constexpr (reflection::is_std_optional_v<Type>) {
        if (t) {
            write(out, true);
            write(out, std::move(t.value()));
        } else {
            write(out, false);
        }
    }
}

template<typename T>
std::decay_t<T> read(iobuf_parser&);

template<typename T>
header_t read_header(iobuf_parser& in) {
    using Type = std::decay_t<T>;

    auto const version = read<version_t>(in);
    auto const compat_version = read<version_t>(in);
    auto const size = read<typename Type::envelope_size_t>(in);

    if (compat_version > Type::__version) {
        serde_log.error(
          "read compat_version={} > {}::version={}\n",
          static_cast<int>(compat_version),
          type_str<T>(),
          static_cast<int>(Type::__version));
        throw std::system_error{
          make_error_code(error_codes::version_older_than_compat_version)};
    }

    if (in.bytes_left() < size) {
        serde_log.error(
          "bytes_left={}, size={}\n", in.bytes_left(), static_cast<int>(size));
        throw std::system_error{
          make_error_code(error_codes::message_too_short)};
    }

    return std::make_tuple(version, compat_version, size);
}

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    static_assert(has_serde_read<T> || is_serde_compatible_v<Type>);

    auto t = Type();
    if constexpr (is_envelope_v<Type>) {
        [[maybe_unused]] auto const [version, compat_version, size] =
          read_header<Type>(in);
        if constexpr (has_serde_read<Type>) {
            t.serde_read(in, version, compat_version, size);
        } else {
            envelope_for_each_field(
              t, [&](auto& f) { f = read<std::decay_t<decltype(f)>>(in); });
        }
    } else if constexpr (std::is_scalar_v<Type>) {
        if (in.bytes_left() < sizeof(Type)) {
            throw std::system_error{
              make_error_code(error_codes::message_too_short)};
        }

        t = ss::le_to_cpu(in.consume_type<Type>());
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        using value_type = typename Type::value_type;
        t.resize(read<int32_t>(in));
        for (auto i = 0U; i < t.size(); ++i) {
            t[i] = read<value_type>(in);
        }
    } else if constexpr (reflection::is_named_type_v<Type>) {
        t = read<typename Type::value_type>(in);
    } else if constexpr (reflection::is_ss_bool_v<Type>) {
        t = read<int8_t>(in);
    } else if constexpr (std::is_same_v<Type, std::chrono::milliseconds>) {
        t = std::chrono::milliseconds(read<int64_t>(in));
    } else if constexpr (std::is_same_v<Type, iobuf>) {
        return in.share(read<int32_t>(in));
    } else if constexpr (std::is_same_v<Type, ss::sstring>) {
        return in.read_string(read<int32_t>(in));
    } else if constexpr (reflection::is_std_optional_v<Type>) {
        return read<bool>(in) ? read<typename Type::value_type>()
                              : std::nullopt;
    }

    return t;
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    if constexpr (has_serde_async_read<Type>) {
        auto const h = read_header<Type>(in);
        return ss::do_with(Type{}, [&in, h](Type& t) {
            auto const& [version, compat_version, size] = h;
            return t.serde_async_read(in, version, compat_version, size)
              .then([&t]() { return std::move(t); });
        });
    } else {
        return ss::make_ready_future<std::decay_t<T>>(read<T>(in));
    }
}

template<typename T>
ss::future<> write_async(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    if constexpr (is_envelope_v<Type> && has_serde_async_write<Type>) {
        write(out, Type::__version);
        write(out, Type::__compat_version);

        auto size_placeholder = out.reserve(
          sizeof(typename Type::envelope_size_t));
        auto const size_before = out.size_bytes();

        return t.serde_async_write(out).then(
          [&out,
           size_before,
           size_placeholder = std::move(size_placeholder)]() mutable {
              auto const written_size = out.size_bytes() - size_before;
              if (
                written_size >
                std::numeric_limits<typename Type::envelope_size_t>::max()) {
                  throw std::system_error{
                    make_error_code(error_codes::envelope_too_big)};
              }
              auto const size = ss::cpu_to_le(
                static_cast<typename Type::envelope_size_t>(written_size));
              size_placeholder.write(
                reinterpret_cast<char const*>(&size),
                sizeof(typename Type::envelope_size_t));

              return ss::make_ready_future<>();
          });
    } else {
        write(out, t);
        return ss::make_ready_future<>();
    }
}

} // namespace serde