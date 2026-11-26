target("RedSocket")
    set_kind("shared")
    set_group("RedSocket")
    set_basename("RedSocket")

    add_files("**.cpp")
    add_headerfiles("**.h", "**.hpp")
    set_pcxxheader("RedSocketPCH.h")
    add_includedirs(".")

    add_deps("RED4ext.SDK", "RedLib")
    add_packages("asio")
    add_links("User32")

    if has_config("running_diagnostics") then
        local sample_interval = get_config("running_diagnostics_sample_interval") or 128
        add_defines("REDSOCKET_RUNNING_DIAGNOSTICS",
                    "REDSOCKET_DIAGNOSTICS_UPDATE_SAMPLE_INTERVAL=" .. sample_interval)
    end

    -- The version header is an audited source input, never rewritten by
    -- configuring or building an extracted source package.
    on_load(function (target)
        local version = get_config("redsocket_version")
        local header = io.readfile(path.join(os.projectdir(), "src", "Version.h"))
        assert(header and header:find('#define REDSOCKET_VERSION "' .. version .. '"', 1, true),
               "Version.h does not match the release configuration")
    end)

    -- Intentionally no install or packaging after-build hook.
target_end()
