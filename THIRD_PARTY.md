# Third-party notices and acknowledgments

DeepBide-RedSocket derives from RedSocket v0.5.0, upstream commit
055420a7317cb6e318251ed168eefad10fb1ea5b, by Louis Poirier (rayshader).
We retain the original MIT copyright and full license in LICENSE and add
copyright (c) 2026 DeepBide only for our modifications.

We sincerely thank each creator and the communities who maintain these
projects. Their work makes ours possible. These acknowledgments are not
a claim of affiliation, endorsement, or ownership of third-party code.

## Pinned build inputs and original notices

| Component and official source | Exact pin and use | License / notice |
| --- | --- | --- |
| [RedSocket](https://github.com/rayshader/cp2077-red-socket) | 055420a7317cb6e318251ed168eefad10fb1ea5b; original implementation and API | MIT; Louis Poirier; LICENSE |
| [RED4ext.SDK](https://github.com/WopsS/RED4ext.SDK) | 0f5eba42a4cf3a1161ad3de0b4ed6f1901ec169e; headers/native bindings | MIT; Octavian Dima; licenses/RED4ext.SDK-LICENSE.md |
| SDK's D3D12 Memory Allocator | SDK's vendored header at the above pin; retain upstream notice conservatively | MIT; Advanced Micro Devices, Inc.; licenses/RED4ext.SDK-THIRD_PARTY_LICENSES.md |
| [RedLib](https://github.com/psiberx/cp2077-red-lib) | 3fd71fee06834e748ea593166fb179c9c1b9191b; native helper headers | MIT; Pavel Siberx; licenses/RedLib-LICENSE |
| [nameof](https://github.com/Neargye/nameof) | RedLib's vendored header at the above pin | MIT; Daniil Goncharov; licenses/nameof-LICENSE |
| [standalone Asio](https://github.com/chriskohlhoff/asio) | 1.32.0, upstream commit 03ae834edbace31a96157b89bf50e5ee464e5ef9; asynchronous networking headers | Boost Software License 1.0; Christopher Kohlhoff and contributors; licenses/Asio-LICENSE_1_0.txt and licenses/Asio-COPYING |

The SDK, RedLib, their transitives and Asio are build dependencies: the
source archive does not vendor their implementations. The compiled
DLL may contain their header-derived code, so the release package
retains their complete original license/notice texts, not merely links.
Original notices are copied from the exact pinned source checkouts.
The DLL import table was inspected; it imports Windows system components
and the Microsoft Visual C++ runtime/UCRT. No Microsoft redistributable,
third-party runtime installer or system DLL is bundled. This inventory is
not a legal opinion.

## Separately installed runtime components

We thank the teams behind [RED4ext](https://github.com/WopsS/RED4ext),
[Cyber Engine Tweaks](https://github.com/maximegmd/CyberEngineTweaks) and
[redscript](https://github.com/jac3km4/redscript). These are external
projects, not distributed in our archives. Consult their respective official
release notes and licenses. The exact tested pairing is RED4ext 1.31.1,
Cyber Engine Tweaks 1.38.1 and redscript 0.5.32; see RELEASE.md. A compatible
Microsoft Visual C++ v14 x64 Redistributable is also required separately. The game itself and its content
remain the property of their respective rights holders.

## Development and tests

Thanks to [Xmake](https://github.com/xmake-io/xmake) (3.1.2 used in lab),
Microsoft's Visual C++ toolchain team, [Python](https://www.python.org/),
[LuaJIT](https://luajit.org/) and [Lupa](https://github.com/scoder/lupa)
for the build and test tools. These tools/runtimes are not bundled.
Test-only dependencies are not an installation requirement. Their own
license terms apply. Thanks also to CD Projekt Red and the wider modding
community for the game and its ecosystem.

This notice must accompany both source and mod packages. The README,
repository description, mod page and release notes should also thank the
upstream maintainers. If future dependencies or bundled files change, update
this inventory and preserve the applicable original notices.
