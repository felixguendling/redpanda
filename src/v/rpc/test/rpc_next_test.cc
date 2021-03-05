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

struct test_msg
  : rpc::envelope<test_msg, rpc::version<4>, rpc::compat_version<0>> {
    std::string _body;
    std::vector<uint32_t> _data;
};

struct not_an_envelope {};
static_assert(!rpc::is_envelope_v<not_an_envelope>);
static_assert(rpc::is_envelope_v<test_msg>);
static_assert(test_msg::version == 4);
static_assert(test_msg::compat_version == 0);

SEASTAR_THREAD_TEST_CASE(envelope_test) {
    auto b = iobuf();

    rpc::write(b, test_msg{._body = "Hello World", ._data = {1, 2, 3}});

    auto parser = iobuf_parser{std::move(b)};

    auto m = test_msg{};
    BOOST_CHECK_NO_THROW(m = rpc::read<test_msg>(parser););
    //    BOOST_CHECK(m._body == "Hello World");
    //    BOOST_CHECK((m._data == std::vector<uint32_t>{1, 2, 3}));
}