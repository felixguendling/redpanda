// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/crc32c.h"
#include "rpc/rpc_next.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>

struct test_msg0
  : rpc::envelope<test_msg0, rpc::version<1>, rpc::compat_version<0>> {
    char _i, _j;
};

struct test_msg1
  : rpc::envelope<test_msg1, rpc::version<4>, rpc::compat_version<0>> {
    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct test_msg1_new
  : rpc::envelope<test_msg1_new, rpc::version<10>, rpc::compat_version<5>> {
    int _a;
    test_msg0 _m;
    int _b, _c;
};

struct not_an_envelope {};
static_assert(!rpc::is_envelope_v<not_an_envelope>);
static_assert(rpc::is_envelope_v<test_msg1>);
static_assert(test_msg1::version == 4);
static_assert(test_msg1::compat_version == 0);

SEASTAR_THREAD_TEST_CASE(reserve_test) {
    auto b = iobuf();
    auto p = b.reserve(10);

    auto const a = std::array<char, 3>{'a', 'b', 'c'};
    p.write(&a[0], a.size());

    auto parser = iobuf_parser{std::move(b)};
    auto called = 0U;
    parser.consume(3, [&](const char* data, size_t max) {
        ++called;
        BOOST_CHECK(max == 3);
        BOOST_CHECK(data[0] == a[0]);
        BOOST_CHECK(data[1] == a[1]);
        BOOST_CHECK(data[2] == a[2]);
        return ss::stop_iteration::no;
    });
    BOOST_CHECK(called == 1);
}

SEASTAR_THREAD_TEST_CASE(simple_envelope_test) {
    struct msg : rpc::envelope<msg, rpc::version<1>, rpc::compat_version<0>> {
        uint32_t _i, _j;
    };

    auto b = iobuf();
    rpc::write(b, msg{._i = 2, ._j = 3});

    auto parser = iobuf_parser{std::move(b)};
    auto m = rpc::read<msg>(parser);
    BOOST_CHECK(m._i == 2);
    BOOST_CHECK(m._j == 3);
}

SEASTAR_THREAD_TEST_CASE(envelope_test) {
    auto b = iobuf();

    rpc::write(
      b, test_msg1{._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    std::cout << "\n\n";
    auto parser = iobuf_parser{std::move(b)};

    auto m = test_msg1{};
    BOOST_CHECK_NO_THROW(m = rpc::read<test_msg1>(parser));
    BOOST_CHECK(m._a == 55);
    BOOST_CHECK(m._b == 33);
    BOOST_CHECK(m._c == 44);
    BOOST_CHECK(m._m._i == 'i');
    BOOST_CHECK(m._m._j == 'j');
}

SEASTAR_THREAD_TEST_CASE(envelope_test_version_older_than_compat_version) {
    auto b = iobuf();

    rpc::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    auto parser = iobuf_parser{std::move(b)};

    auto throws = false;
    try {
        rpc::read<test_msg1>(parser);
    } catch (std::system_error const& e) {
        BOOST_CHECK(
          e.code()
          == rpc::make_error_code(
            rpc::rpc_error_codes::version_older_than_compat_version));
        throws = true;
    }

    BOOST_CHECK(throws);
}

SEASTAR_THREAD_TEST_CASE(envelope_test_buffer_too_short) {
    auto b = iobuf();

    rpc::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    b.pop_back(); // introduce length mismatch
    auto parser = iobuf_parser{std::move(b)};

    auto throws = false;
    try {
        rpc::read<test_msg1_new>(parser);
    } catch (std::system_error const& e) {
        throws = true;
    }
    BOOST_CHECK(throws);
}

SEASTAR_THREAD_TEST_CASE(vector_test) {
    auto b = iobuf();

    rpc::write(b, std::vector{1, 2, 3});

    auto parser = iobuf_parser{std::move(b)};
    auto const m = rpc::read<std::vector<int>>(parser);
    BOOST_CHECK((m == std::vector{1, 2, 3}));
}

// struct with differing sizes:
// vector length may take different size (vint)
// vector data may have different size (_ints.size() * sizeof(int))
struct inner_differing_sizes
  : rpc::envelope<inner_differing_sizes, rpc::version<1>> {
    std::vector<int32_t> _ints;
};

struct complex_msg : rpc::envelope<complex_msg, rpc::version<3>> {
    std::vector<inner_differing_sizes> _vec;
    int32_t _x;
};

SEASTAR_THREAD_TEST_CASE(complex_msg_test) {
    auto b = iobuf();

    inner_differing_sizes big;
    big._ints.resize(std::numeric_limits<uint16_t>::max() + 1);
    std::fill(begin(big._ints), end(big._ints), 4);

    rpc::write(
      b,
      complex_msg{
        ._vec = std::
          vector{inner_differing_sizes{._ints = {1, 2, 3}}, big, inner_differing_sizes{._ints = {1, 2, 3}}, big, inner_differing_sizes{._ints = {1, 2, 3}}, big},
        ._x = 3});

    auto parser = iobuf_parser{std::move(b)};
    auto const m = rpc::read<complex_msg>(parser);
    BOOST_CHECK(m._vec.size() == 6);
    for (auto i = 0U; i < m._vec.size(); ++i) {
        if (i % 2 == 0) {
            BOOST_CHECK((m._vec[i]._ints == std::vector{1, 2, 3}));
        } else {
            BOOST_CHECK(m._vec[i]._ints == big._ints);
        }
    }
}

struct test_snapshot_header
  : public rpc::envelope<test_snapshot_header, rpc::version<1>> {
    uint32_t header_crc;
    uint32_t metadata_crc;
    int8_t version;
    int32_t metadata_size;
};

template<
  typename T,
  rpc::mode M = rpc::mode::SYNC,
  std::enable_if_t<
    M == rpc::mode::ASYNC
      && std::is_same_v<test_snapshot_header, std::decay_t<T>>,
    void*> = nullptr>
auto read(iobuf_parser& in) -> ss::future<std::decay_t<test_snapshot_header>> {
    test_snapshot_header hdr;
    hdr.header_crc = rpc::read<decltype(hdr.header_crc)>(in);
    hdr.metadata_crc = rpc::read<decltype(hdr.metadata_crc)>(in);
    hdr.version = rpc::read<decltype(hdr.version)>(in);
    hdr.metadata_size = rpc::read<decltype(hdr.metadata_size)>(in);

    vassert(
      hdr.metadata_size >= 0, "Invalid metadata size {}", hdr.metadata_size);

    crc32 crc;
    crc.extend(ss::cpu_to_le(hdr.metadata_crc));
    crc.extend(ss::cpu_to_le(hdr.version));
    crc.extend(ss::cpu_to_le(hdr.metadata_size));

    if (hdr.header_crc != crc.value()) {
        return ss::make_exception_future<test_snapshot_header>(
          std::runtime_error(fmt::format(
            "Corrupt snapshot. Failed to verify header crc: {} != "
            "{}: path?",
            crc.value(),
            hdr.header_crc)));
    }

    return ss::make_ready_future<test_snapshot_header>(hdr);
}

SEASTAR_THREAD_TEST_CASE(snapshot_test) {
    auto b = iobuf();
    rpc::write(
      b,
      test_snapshot_header{
        .header_crc = 1, .metadata_crc = 2, .version = 3, .metadata_size = 4});
    auto parser = iobuf_parser{std::move(b)};
    auto const f = read<test_snapshot_header, rpc::mode::ASYNC>(parser);
    BOOST_CHECK(f.failed());
}