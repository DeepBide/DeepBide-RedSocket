-- Build configurations
add_rules("mode.debug", "mode.release")

set_policy("build.ccache", false)
set_policy("package.requires_lock", true)

set_config("redsocket_version", "0.5.2-rc.3")

option("running_diagnostics")
    set_default(false)
    set_showmenu(true)
    set_description("Enable bounded RED4ext Running callback diagnostics (lab/diagnostic builds only)")
option_end()

option("running_diagnostics_sample_interval")
    set_default(128)
    set_showmenu(true)
    set_values(16, 32, 64, 128, 256, 512, 1024)
    set_description("Emit one callback update sample every N Running updates")
option_end()

-- Languages
set_languages("c99", "cxx20")

-- Dependencies
add_requires("asio 1.32.0")

-- Compilation
set_warnings("all")

if is_plat("windows") then
    add_defines("NOMINMAX", "REDSOCKET_WINDOWS")
end

if is_mode("debug") then
    add_defines("REDSOCKET_DEBUG")
elseif is_mode("release") then
    add_defines("REDSOCKET_RELEASE")
end

includes("src")
includes("vendors")
includes("tests")
