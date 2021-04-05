#pragma once

#include <system_error>

namespace serde {

enum class error_codes : int {
    version_older_than_compat_version,
    message_too_short,
    envelope_too_big
};

struct error_category final : std::error_category {
    const char* name() const noexcept final;
    std::string message(int ec) const final;
};

std::error_code make_error_code(error_codes ec) noexcept;

} // namespace serde