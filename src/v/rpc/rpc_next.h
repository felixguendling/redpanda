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
#include "reflection/for_each_field.h"
#include "rpc/logger.h"
#include "utils/named_type.h"
#include "utils/vint.h"

#include <iosfwd>
#include <numeric>
#include <string>
#include <string_view>

namespace cista {

#if defined(_MSC_VER)
#define CISTA_SIG __FUNCSIG__
#elif defined(__clang__) || defined(__GNUC__)
#define CISTA_SIG __PRETTY_FUNCTION__
#else
#error unsupported compiler
#endif

template<typename T>
constexpr std::string_view type_str() {
#if defined(__clang__)
    constexpr std::string_view prefix
      = "std::string_view cista::type_str() [T = ";
    constexpr std::string_view suffix = "]";
#elif defined(_MSC_VER)
    constexpr std::string_view prefix
      = "class std::basic_string_view<char,struct std::char_traits<char> > "
        "__cdecl cista::type_str<";
    constexpr std::string_view suffix = ">(void)";
#else
    constexpr std::string_view prefix
      = "constexpr std::string_view cista::type_str() [with T = ";
    constexpr std::string_view suffix
      = "; std::string_view = std::basic_string_view<char>]";
#endif

    auto sig = std::string_view{CISTA_SIG};
    sig.remove_prefix(prefix.size());
    sig.remove_suffix(suffix.size());
    return sig;
}

} // namespace cista

namespace rpc {

namespace detail {

struct instance {
    template<typename Type>
    operator Type() const;
};

template<
  typename Aggregate,
  typename IndexSequence = std::index_sequence<>,
  typename = void>
struct arity_impl : IndexSequence {};

template<typename Aggregate, std::size_t... Indices>
struct arity_impl<
  Aggregate,
  std::index_sequence<Indices...>,
  std::void_t<decltype(Aggregate{
    (static_cast<void>(Indices), std::declval<instance>())...,
    std::declval<instance>()})>>
  : arity_impl<Aggregate, std::index_sequence<Indices..., sizeof...(Indices)>> {
};

} // namespace detail

template<typename T>
constexpr std::size_t arity() {
    return detail::arity_impl<std::decay_t<T>>().size();
}

template<typename T>
inline auto envelope_to_tuple(T& t) {
    constexpr auto const a = arity<T>() - 1;
    static_assert(a <= 8, "Max. supported members: 64");
    if constexpr (a == 1) {
        auto& [p1] = t;
        return std::tie(p1);
    } else if constexpr (a == 2) {
        auto& [p1, p2] = t;
        return std::tie(p1, p2);
    } else if constexpr (a == 3) {
        auto& [p1, p2, p3] = t;
        return std::tie(p1, p2, p3);
    } else if constexpr (a == 4) {
        auto& [p1, p2, p3, p4] = t;
        return std::tie(p1, p2, p3, p4);
    } else if constexpr (a == 5) {
        auto& [p1, p2, p3, p4, p5] = t;
        return std::tie(p1, p2, p3, p4, p5);
    } else if constexpr (a == 6) {
        auto& [p1, p2, p3, p4, p5, p6] = t;
        return std::tie(p1, p2, p3, p4, p5, p6);
    } else if constexpr (a == 7) {
        auto& [p1, p2, p3, p4, p5, p6, p7] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7);
    } else if constexpr (a == 8) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8);
    }
}

using version_t = uint8_t;

template<version_t V>
struct version {
    static constexpr auto const v = V;
};

template<version_t V>
struct compat_version {
    static constexpr auto const v = V;
};

template<unsigned N>
struct fixed_str {
    char buf[N + 1]{};
    constexpr fixed_str(char const* s) {
        for (auto i = 0U; i != N; ++i) {
            buf[i] = s[i];
        }
    }
    constexpr operator char const *() const { return buf; }
};
template<unsigned N>
fixed_str(char const (&)[N]) -> fixed_str<N - 1>;

template<
  typename T,
  auto FieldName = "unnamed",
  typename MinVersion = version<0>,
  typename MaxVersion = version<std::numeric_limits<version_t>::max()>>
struct field {
    static constexpr char const* Name = FieldName;
    static constexpr int MinV = MinVersion::v;
    static constexpr int MaxV = MaxVersion::v;
    field() = default;
    field(T v)
      : _val{std::forward<T>(v)} {}
    operator T const &() { return _val; }
    T _val;
};

/**
 * \brief provides versioning for serializable types.
 *
 * It reads/writes version and compat version and throws
 * if the version is lower than the compat version on reading.
 *
 * \tparam Version         the current type version
 *                         (change for every incompatible update)
 * \tparam CompatVersion   the minimum required version able to parse the type
 */
template<typename T, typename Version, typename CompatVersion = Version>
struct envelope {
    using value_t = T;
    static constexpr auto version = Version::v;
    static constexpr auto compat_version = CompatVersion::v;
};

template<typename T>
struct inherits_from_envelope {
    static constexpr auto const value = std::is_base_of_v<
      envelope<T, version<T::version>, compat_version<T::compat_version>>,
      T>;
};

template<typename T, typename = void>
struct is_envelope : std::false_type {};

template<typename T>
struct is_envelope<T, std::void_t<decltype(std::declval<T>().compat_version)>>
  : std::true_type {};

template<typename T>
inline constexpr auto const is_envelope_v = is_envelope<T>::value;

template<typename T, typename Fn>
inline void envelope_for_each_field(T& t, Fn&& fn) {
    static_assert(is_envelope_v<std::decay_t<T>>);
    std::apply([&](auto&&... args) { (fn(args), ...); }, envelope_to_tuple(t));
}

// START copied from adl.h
template<typename T>
struct is_std_vector : std::false_type {};
template<typename... Args>
struct is_std_vector<std::vector<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

template<typename T>
struct is_std_optional : std::false_type {};
template<typename... Args>
struct is_std_optional<std::optional<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_optional_v = is_std_optional<T>::value;

template<typename T>
struct is_named_type : std::false_type {};
template<typename T, typename Tag>
struct is_named_type<named_type<T, Tag>> : std::true_type {};
template<typename T>
inline constexpr bool is_named_type_v = is_named_type<T>::value;

template<typename T>
struct is_ss_bool : std::false_type {};
template<typename T>
struct is_ss_bool<ss::bool_class<T>> : std::true_type {};
template<typename T>
inline constexpr bool is_ss_bool_v = is_ss_bool<T>::value;
// END copied from adl.h

template<typename T>
inline constexpr auto const is_serializable_v
  = is_std_vector_v<T> || std::is_scalar_v<T> || is_envelope_v<T>;

template<typename T>
size_t binary_size(T const& t) {
    using Type = std::decay_t<T>;
    static_assert(is_serializable_v<Type>);
    if constexpr (is_envelope_v<T>) {
        auto sum = size_t{};
        envelope_for_each_field(t, [&sum](auto& f) { sum += binary_size(f); });
        return sum;
    } else if constexpr (is_std_vector_v<Type>) {
        auto sum = vint::vint_size(t.size());
        for (auto const& el : t) {
            sum += binary_size(el);
        }
        return sum;
    } else if constexpr (std::is_scalar_v<Type>) {
        return sizeof(Type);
    }
}

template<typename T>
void write(iobuf& out, T const& t) {
    using Type = std::decay_t<T>;
    static_assert(is_serializable_v<Type>);
    if constexpr (is_envelope_v<Type>) {
        auto size_buf = std::array<uint8_t, 9>{};
        auto const size_size = vint::serialize(binary_size(t), &size_buf[0]);
        out.append(&size_buf[0], size_size);

        write(out, T::version);
        write(out, T::compat_version);

        envelope_for_each_field(t, [&out](auto& f) { write(out, f); });
    } else if constexpr (std::is_scalar_v<Type>) {
        auto le_t = ss::cpu_to_le(t);
        out.append(reinterpret_cast<const char*>(&le_t), sizeof(Type));
    } else if constexpr (is_std_vector_v<Type>) {
        auto size_buf = std::array<uint8_t, 9>{};
        auto const size_size = vint::serialize(binary_size(t), &size_buf[0]);
        out.append(&size_buf[0], size_size);
        for (auto const& el : t) {
            write(out, el);
        }
    }
}

enum class rpc_error_codes : int {
    version_older_than_compat_version,
    message_too_short
};

struct rpc_error_category final : std::error_category {
    const char* name() const noexcept final { return "rpc"; }
    std::string message(int ec) const final {
        switch (static_cast<rpc_error_codes>(ec)) {
        case rpc_error_codes::version_older_than_compat_version:
            return "Code message version is older than compatability version.";
        case rpc_error_codes::message_too_short:
            return "Message length shorter than communicated message length.";
        }
        return "unknown";
    }
};

std::error_code make_error_code(rpc_error_codes ec) noexcept {
    static rpc_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    using Type = std::decay_t<T>;
    auto t = Type();

    if (in.bytes_left() < sizeof(Type)) {
        throw std::system_error{
          make_error_code(rpc_error_codes::message_too_short)};
    }

    if constexpr (is_envelope_v<Type>) {
        // Read envelope header: version, compat version, and size.
        auto const version = read<version_t>(in);
        auto const compat_version = read<version_t>(in);
        auto const [size, size_size] = in.read_varlong();

        (void)version;

        if (compat_version > Type::version) {
            rpclog.error(
              "read compat_version={} > {}::version={}\n ",
              static_cast<int>(compat_version),
              cista::type_str<T>(),
              static_cast<int>(Type::version));
            throw std::system_error{make_error_code(
              rpc_error_codes::version_older_than_compat_version)};
        }

        if (in.bytes_left() < size) {
            throw std::system_error{
              make_error_code(rpc_error_codes::message_too_short)};
        }

        envelope_for_each_field(
          t, [&](auto& field) { field = read<decltype(field)>(in); });
    } else if constexpr (std::is_scalar_v<Type>) {
        t = ss::le_to_cpu(in.consume_type<Type>());
    }
    return t;
}

} // namespace rpc