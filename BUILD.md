# Build DeepBide-RedSocket in an isolated directory

This builds the frozen 0.5.2-rc.3 runtime used for the 0.5.2 stable distribution. Windows x64,
Visual Studio 2022 C++ tools, xmake 3.1.2, Git and an independent output
directory are needed. No installer hooks or game files are used.

## Source dependencies

The standalone source archive does not vendor dependency implementations.
Populate the following vendor directories
with the exact public commits (do not use moving branches or latest tags).
Run from the root of the standalone source tree:

```powershell
git clone https://github.com/WopsS/RED4ext.SDK.git vendors/RED4ext.SDK
git -C vendors/RED4ext.SDK checkout --detach 0f5eba42a4cf3a1161ad3de0b4ed6f1901ec169e
git clone https://github.com/psiberx/cp2077-red-lib.git vendors/RedLib
git -C vendors/RedLib checkout --detach 3fd71fee06834e748ea593166fb179c9c1b9191b
git -C vendors/RED4ext.SDK rev-parse HEAD
git -C vendors/RedLib rev-parse HEAD
```

Require both exact commits and clean trees before compilation. Both include
their recorded transitive vendored headers. Xmake's asio 1.32.0 dependency
and package-repository commit are pinned in xmake-requires.lock; compare
the resolved headers against standalone Asio upstream commit
03ae834edbace31a96157b89bf50e5ee464e5ef9 for release provenance.
Use the full license texts in licenses/ for redistribution. Do not install
or copy a DLL into the game from the build.

## Build and exercise the transport helpers

Choose fresh lab directories separate from the game. For example:

```powershell
$env:XMAKE_GLOBALDIR = Join-Path $env:TEMP "deepbide-redsocket-xmake-review"
$build = Join-Path $env:TEMP "deepbide-redsocket-build-review"
xmake config -p windows -a x64 -m release -o $build -y --running_diagnostics=n
xmake build RedSocket
xmake build deepbide-transport-contract
& (Join-Path $build "windows/x64/release/deepbide-transport-contract.exe")
```

The DLL is at windows/x64/release/RedSocket.dll inside the chosen build
directory. Hash the DLL and exact source revision; compiler outputs are
not claimed bit-reproducible across machines. The transport contract
harness exercises production framing and buffer ownership helpers and a
worker reap, via ephemeral loopback TCP, not native game bindings.
For the optional CET wrapper tests use Python 3.11+ and Lupa 2.8 with
LuaJIT 2.1; run python tests/test_cet_wrapper.py in the source tree.
The remaining native harnesses cover owner-loopback, handle lifetime,
Running callbacks and diagnostics. The loopback harness uses ephemeral
ports, leaving actual clients and their listening ports untouched.
Build deepbide-production-contract to exercise the actual RedSocket.cpp
and SocketService.cpp with deliberately narrow native API doubles and
real Asio loopback. The doubles do not implement the real game ABI.
The contract includes fault injection at command/finalizer submission and
resolve/write initiation, plus the actual socket-cap admission limit.
Run it also with --external-unload for the barrier-controlled lifecycle test.
The fault-injection hooks exist only in the dedicated test target.
No test proves actual loader callback affinity or safe in-game unload.

The packaged DLL was built with MSVC 19.44.35229.0 (toolset 14.44.35207),
xmake 3.1.2, Windows x64 release, running diagnostics disabled, from the
exact source inputs represented by build_input_sha256 in the release
manifest. The pinned installed Asio headers (630 files) matched the
upstream Git blobs byte for byte. The published documentation was updated
after that build; no executable source or bridge changed. The reviewed DLL
was not recompiled for packaging. See RELEASE.md for physical evidence.
