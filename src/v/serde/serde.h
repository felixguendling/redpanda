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
    inline constexpr auto const SERDE_TOKEN_PASTE(has_, name)                  \
      = SERDE_TOKEN_PASTE(help_has_, name)<T>::value;

static auto iob = iobuf{};
static auto iop = iobuf_parser{std::move(iob)};

HAS_MEMBER_FN(serde_read, iop)
HAS_MEMBER_FN(serde_write, iob)
HAS_MEMBER_FN(serde_async_read, iop)
HAS_MEMBER_FN(serde_async_write, iob)

template<typename T>
inline constexpr auto const is_serializable_v
  = reflection::is_std_vector_v<T> || std::is_scalar_v<T> || is_envelope_v<T>;

template<typename T>
void write(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    static_assert(is_serializable_v<Type>);

    if constexpr (has_serde_read<Type>) {
        t.write(out);
    } else if constexpr (is_envelope_v<Type>) {
        write(out, Type::__version);
        write(out, Type::__compat_version);

        auto size_placeholder = out.reserve(
          sizeof(typename Type::envelope_size_t));
        auto const size_before = out.size_bytes();

        envelope_for_each_field(t, [&out](auto& f) { write(out, f); });

        auto const written_size = out.size_bytes() - size_before;
        if (
          written_size
          > std::numeric_limits<typename Type::envelope_size_t>::max()) {
            throw std::system_error{
              make_error_code(error_codes::envelope_too_big)};
        }
        auto const size = ss::cpu_to_le(
          static_cast<typename Type::envelope_size_t>(written_size));
        size_placeholder.write(
          reinterpret_cast<char const*>(&size), sizeof(size));
    } else if constexpr (std::is_scalar_v<Type>) {
        auto le_t = ss::cpu_to_le(t);
        out.append(reinterpret_cast<char const*>(&le_t), sizeof(Type));
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        assert(t.size() <= std::numeric_limits<uint32_t>::max());
        write(out, static_cast<uint32_t>(t.size()));
        for (auto const& el : t) {
            write(out, el);
        }
    }
}

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    auto t = Type();

    if constexpr (has_serde_read<Type>) {
        t.read(in);
    } else if constexpr (is_envelope_v<Type>) {
        // Read envelope header: version, compat version, and size.
        auto const version = read<version_t>(in);
        auto const compat_version = read<version_t>(in);
        auto const size = read<typename Type::envelope_size_t>(in);
        (void)version; // could be used to drop messages from old versions

        if (compat_version > Type::__version) {
            serde_log.error(
              "read compat_version={} > {}::version={}\n ",
              static_cast<int>(compat_version),
              type_str<T>(),
              static_cast<int>(Type::__version));
            throw std::system_error{
              make_error_code(error_codes::version_older_than_compat_version)};
        }

        if (in.bytes_left() < size) {
            throw std::system_error{
              make_error_code(error_codes::message_too_short)};
        }

        envelope_for_each_field(
          t, [&](auto& f) { f = read<std::decay_t<decltype(f)>>(in); });
    } else if constexpr (std::is_scalar_v<Type>) {
        if (in.bytes_left() < sizeof(Type)) {
            throw std::system_error{
              make_error_code(error_codes::message_too_short)};
        }

        t = ss::le_to_cpu(in.consume_type<Type>());
    } else if constexpr (reflection::is_std_vector_v<Type>) {
        using value_type = typename Type::value_type;
        t.resize(read<uint32_t>(in));
        for (auto i = 0U; i < t.size(); ++i) {
            t[i] = read<value_type>(in);
        }
    }

    return t;
}

template<typename T>
ss::future<std::decay_t<T>> read_async(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    if constexpr (has_serde_async_read<Type>) {
        auto const t = std::make_shared<Type>();
        return t->serde_async_read(in).then([t]() { return *t; });
    } else {
        return ss::make_ready_future<std::decay_t<T>>(read<T>(in));
    }
}

template<typename T>
ss::future<> write_async(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    if constexpr (has_serde_async_write<Type>) {
        return t.serde_async_write(out);
    } else {
        write(out, t);
        return ss::make_ready_future<>();
    }
}

} // namespace serde