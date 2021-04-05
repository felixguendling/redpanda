#pragma once

#include <cinttypes>
#include <type_traits>

namespace serde {

using version_t = uint8_t;

template<version_t V>
struct version {
    static constexpr auto const v = V;
};

template<version_t V>
struct compat_version {
    static constexpr auto const v = V;
};

/**
 * \brief provides versioning (version + compat version)
 *        for serializable aggregate types.
 *
 * \tparam Version         the current type version
 *                         (change for every incompatible update)
 * \tparam CompatVersion   the minimum required version able to parse the type
 */
template<
  typename T,
  typename Version,
  typename CompatVersion = compat_version<Version::v>>
struct envelope {
    using envelope_size_t = uint32_t;
    using value_t = T;
    static constexpr auto __version = Version::v;
    static constexpr auto __compat_version = CompatVersion::v;
};

namespace detail {

template<typename T>
struct inherits_from_envelope {
    using Type = std::decay_t<T>;
    static constexpr auto const value = std::is_base_of_v<
      envelope<
        Type,
        version<Type::__version>,
        compat_version<Type::__compat_version>>,
      Type>;
};

template<typename T, typename = void>
struct has_compat_attribute : std::false_type {};

template<typename T>
struct has_compat_attribute<
  T,
  std::void_t<decltype(std::declval<T>().__compat_version)>>
  : std::true_type {};

} // namespace detail

template<typename T>
inline constexpr auto const is_envelope_v = std::conjunction_v<
  detail::has_compat_attribute<T>,
  detail::inherits_from_envelope<T>>;

} // namespace serde