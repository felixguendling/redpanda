// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//#include <boost/test/tools/old/interface.hpp>

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
  : rpc::envelope<test_msg1, rpc::version<10>, rpc::compat_version<5>> {
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

    auto m = rpc::read<test_msg1>(parser);
    BOOST_CHECK(m.has_value());
    BOOST_CHECK(m.value()._a == 55);
    BOOST_CHECK(m.value()._b == 33);
    BOOST_CHECK(m.value()._c == 44);
    BOOST_CHECK(m.value()._m._i == 'i');
    BOOST_CHECK(m.value()._m._j == 'j');
}

SEASTAR_THREAD_TEST_CASE(envelope_test_version_older_than_compat_version) {
    auto b = iobuf();

    rpc::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    auto parser = iobuf_parser{std::move(b)};

    auto m = rpc::read<test_msg1>(parser);
    BOOST_CHECK(m.has_error());
    BOOST_CHECK(
      m.error().value()
      == static_cast<int>(
        rpc::rpc_error_codes::version_older_than_compat_version));
}

SEASTAR_THREAD_TEST_CASE(envelope_test_buffer_too_short) {
    auto b = iobuf();

    rpc::write(
      b,
      test_msg1_new{
        ._a = 55, ._m = {._i = 'i', ._j = 'j'}, ._b = 33, ._c = 44});

    b.pop_back(); // introduce length mismatch
    auto parser = iobuf_parser{std::move(b)};

    auto m = rpc::read<test_msg1>(parser);
    BOOST_CHECK(m.has_exception());
}