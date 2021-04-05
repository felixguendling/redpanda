#pragma once

#include "reflection/arity.h"

#include <tuple>

namespace serde {

template<typename T>
inline auto envelope_to_tuple(T& t) {
    constexpr auto const a = reflection::arity<T>() - 1;
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

template<typename T, typename Fn>
inline void envelope_for_each_field(T& t, Fn&& fn) {
    static_assert(is_envelope_v<std::decay_t<T>>);
    std::apply([&](auto&&... args) { (fn(args), ...); }, envelope_to_tuple(t));
}

} // namespace serde