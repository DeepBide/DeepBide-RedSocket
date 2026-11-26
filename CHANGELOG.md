# DeepBide-RedSocket changelog

## 0.5.2-rc.3 — 2026-11-26 — first public release candidate

Forked from RedSocket v0.5.0 by Louis Poirier (rayshader). The changes below
describe the maintained fork, not the upstream project's release history.

- Retains the RedSocket module, RTTI, DLL and script paths for compatibility.
- Independent socket ownership, bounded command and event queues, explicit
  listener admission and retirement; individual destruction is idempotent.
- LF/CRLF receive framing, CRLF send framing and strict single-line UTF-8
  validation, with bounded parsing and queue accounting.
- Separate transport generations and retained asynchronous operation buffers;
  stale completions cannot mutate replacement connections.
- Corrected callback and send-error ownership, clean EOF handling and
  isolation between consumers.
- Worker shutdown, cancellation draining and allocation/submission fault
  recovery; normal loader-driven unload supported on the tested pairing.
- Standalone pinned build, native and Lua regression suites, release manifests,
  original upstream MIT copyright, six third-party notices and acknowledgments.

Validation: see RELEASE.md for the exact CP2077 2.5 pairing, audit scope,
transport/reconnect/ordinary-exit evidence and remaining limitations.
No throughput, FPS, prolonged stability or forced-unload guarantee.

Thank you to Louis Poirier and every upstream creator, maintainer and
community listed in THIRD_PARTY.md. This release builds on their work.
