// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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

SEASTAR_THREAD_TEST_CASE(envelope_test) {
    auto b = iobuf();

    rpc::write(
      b, test_msg1{._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

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

    rpc::write(
      b,
      complex_msg{
        ._vec = std::
          vector{inner_differing_sizes{._ints = {1, 2, 3}}, big, inner_differing_sizes{._ints = {2, 3, 4}}, big, inner_differing_sizes{._ints = {3, 4, 5}}, big},
        ._x = 3});

    auto parser = iobuf_parser{std::move(b)};
    auto const m = complex_msg{};
    rpc::read<complex_msg>(parser);
    BOOST_CHECK(m._vec.size() == 6);
}