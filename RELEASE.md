# DeepBide-RedSocket 0.5.2-rc.3

**Release candidate / pre-release.** Windows x64. This is a maintained
fork of RedSocket, not a new wire protocol or an official endorsement.

## Requirements — exact tested environment

| Component | Version |
| --- | --- |
| Cyberpunk 2077 | 2.5 (file version 3.0.88.46235), Windows x64 |
| [RED4ext](https://github.com/WopsS/RED4ext/releases/tag/v1.31.1) | 1.31.1 |
| [Cyber Engine Tweaks](https://github.com/maximegmd/CyberEngineTweaks/releases/tag/v1.38.1) | 1.38.1 |
| [redscript](https://github.com/jac3km4/redscript/releases/tag/v0.5.32) | 0.5.32 |
| Microsoft Visual C++ runtime | compatible v14 x64 redistributable, installed separately |

Install these dependencies from their official sources. The Microsoft
runtime is required by the DLL imports (MSVCP140, VCRUNTIME140,
VCRUNTIME140_1 and UCRT); see
https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist.
No dependency installer or game content is bundled. Other versions or
platforms, including the redscript 1.x compiler generation, were not
qualified. Build SDK/RedLib/Asio pins are distinct from runtime versions.

## Install, upgrade and remove

The fork deliberately retains RedSocket's original paths and API names.
It **replaces** RedSocket; do not install the original and this fork side
by side. Back up your existing RedSocket installation outside the game
directory and record which files were absent. Close the game and launcher
before changing files. Do not overwrite another mod's files.

Verify the downloaded mod ZIP against its published SHA-256 and each
member against release-manifest.json. Extract the mod ZIP into a temporary
directory and copy the contents into the Cyberpunk 2077 root (not into an
extra directory with the ZIP name). The four runtime files are:

- red4ext/plugins/RedSocket/RedSocket.dll
- bin/x64/plugins/cyber_engine_tweaks/mods/RedSocket/RedSocket.lua
- bin/x64/plugins/cyber_engine_tweaks/mods/RedSocket/init.lua
- r6/scripts/RedSocket.reds

The accompanying documentation and licenses reside in
red4ext/plugins/RedSocket/. The root release-manifest.json is a verification
record; keep it outside the game. Use the DLL and all three bridges from
the same archive. Do not install the source archive as a mod.
The legacy red.config.json is not a release installer.

Start the game normally and verify the full RC identity in the RedSocket
log. The loader's numeric version is 0.5.2; the CET wrapper and startup
log identify 0.5.2-rc.3. A matching numeric version alone is not proof of
matching files. If a dependent mod or script fails, close the game
normally. To uninstall or roll back, stop the game and launcher, remove
only the files added by this archive that were originally absent, and
restore only the backed-up files previously present. Verify hashes and
absences; stop if a file was unexpectedly changed. Leave other mods,
configurations, game-generated caches and saves alone.

## Qualification and limitations

An independent 25-criterion source audit was performed on the earlier
RC1; all 16 findings have scoped remediation evidence in RC3. RC3 was not
subject to a second independent audit. Native Windows compilation,
standalone source compilation, bounded transport and production-route
tests (using native API doubles), listener lifecycle and fault injection,
and seven CET/Lua tests passed.

On 26 November 2026, in an approximately five-minute session at the main
menu, the actual game loaded this exact DLL and official dependencies.
Two independent clients used the CET and REDscript interfaces. A controlled
EOF on one connection was followed by reconnection in five seconds, while
the second connection remained active and answered a second challenge.
631 envelopes (505 position envelopes and 126 heartbeats) were received,
560 after the intentional disconnect, with zero invalid frames or transport
errors in the witness. Normal game exit completed terminal plugin cleanup
and the loader explicitly reported unload. The four runtime files were
restored to their exact preimages afterward.

This is scoped transport/ordinary-shutdown evidence, not validation of
the consumers' gameplay data or visual output. No comparative gameplay
FPS, prolonged soak, exhaustive allocator/host-thread-affinity coverage or
physical double-destroy certification is claimed. Do not hot-reload or
forcibly unload the DLL. RED4ext 1.31.1 ignores the plugin Main(Unload)
return value; returning false is not an unload veto. See the
[loader source](https://github.com/WopsS/RED4ext/blob/d542a3beaa33816f1bbf20fb73d7cee604a754d6/src/dll/Systems/PluginSystem.cpp)
and its [ordinary shutdown hook](https://github.com/WopsS/RED4ext/blob/d542a3beaa33816f1bbf20fb73d7cee604a754d6/src/dll/Hooks/QuickExit.cpp).
There is no hard shutdown deadline.

## Source and artifact provenance

The matching source archive contains source, tests, build instructions,
license and notices. release-manifest.json records the complete source-tree
digest, the build-input digest excluding release documentation, each
member SHA-256, dependency pins and the tested DLL SHA-256:
40c76ec43cf56f16ca40d7441e8bdd108d1794b4d52cca00110b57328ec9a245.
The runtime files are byte-identical to the physically tested candidate.
No bit-reproducible native compiler output is claimed across machines.
See BUILD.md and THIRD_PARTY.md.

## Acknowledgments

Our sincere thanks to Louis Poirier (rayshader), RED4ext/SDK, RedLib, Asio,
nameof, CET, redscript, Xmake and the other creators, maintainers and
communities recognized in THIRD_PARTY.md. Their work makes this fork possible.
