#include "trading/dashboard_server.hpp"

namespace trading {
namespace {

// Generated at build time from app/dashboard.html (see cmake/embed_page.cmake), so the
// demo binary serves the page without depending on its working directory.
constexpr unsigned char kPage[] = {
#include "dashboard_page.inc"
};

}  // namespace

std::string_view embedded_dashboard_page() noexcept {
    return {reinterpret_cast<const char*>(kPage), sizeof(kPage)};
}

}  // namespace trading
