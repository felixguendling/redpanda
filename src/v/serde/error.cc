#include "serde/error.h"

namespace serde {

const char* error_category::name() const noexcept { return "rpc"; }

std::string error_category::message(int ec) const {
    switch (static_cast<error_codes>(ec)) {
    case error_codes::version_older_than_compat_version:
        return "Code message version is older than compatability version.";
    case error_codes::message_too_short:
        return "Message length shorter than communicated message length.";
    case error_codes::envelope_too_big:
        return "Message length shorter than communicated message length.";
    }
    return "unknown";
}

std::error_code make_error_code(error_codes ec) noexcept {
    static error_category ecat;
    return {static_cast<int>(ec), ecat};
}

} // namespace serde