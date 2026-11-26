target("RED4ext.SDK")
    set_kind("headeronly")
    set_group("Vendors")

    -- Files
    add_files("RED4ext.SDK/src/**.cpp")
    add_headerfiles("RED4ext.SDK/include/**.hpp")

    add_includedirs("RED4ext.SDK/include/", { public = true })

    -- Configuration
    add_defines("RED4EXT_HEADER_ONLY")
target_end()

target("RedLib")
    set_kind("headeronly")
    set_group("Vendors")

    -- Files
    add_headerfiles("RedLib/include/**.hpp")

    add_includedirs("RedLib/include/", { public = true })
    add_includedirs("RedLib/vendor/", { public = true })
    -- RED4ext/Common.hpp already defines RED4EXT_OFFSET_TO_ADDR as a function macro.
    -- Never predefine it as a bare compiler flag: that suppresses the SDK implementation.
target_end()