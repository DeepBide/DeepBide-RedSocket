# DeepBide-RedSocket

An independently usable TCP client mod for Cyberpunk 2077, maintained by
DeepBide from Louis Poirier's MIT-licensed RedSocket v0.5.0.
Version **0.5.2-rc.3** — first public release candidate for Windows x64,
with scoped in-game validation on Cyberpunk 2077 2.5. Not a stable release.
See the tested pairing and limitations in [RELEASE.md](RELEASE.md).

## Acknowledgments

Our sincere thanks to **Louis Poirier (rayshader)**, the creator of
[RedSocket](https://github.com/rayshader/cp2077-red-socket), whose original
work made this fork possible. We also warmly thank **Octavian Dima and
the RED4ext/SDK contributors**, **Pavel Siberx and the RedLib contributors**,
**Christopher Kohlhoff and the Asio community**, **Daniil Goncharov and
nameof contributors**, and the teams and communities behind **Cyber Engine
Tweaks, redscript, Xmake, LuaJIT, Lupa and Python**. Thanks also to the AMD
D3D12 Memory Allocator contributors whose notice is retained from the SDK,
and to the wider Cyberpunk modding community for their research, testing,
documentation and generosity.

Please visit the original projects linked in [THIRD_PARTY.md](THIRD_PARTY.md)
and support their maintainers. DeepBide's work builds on theirs; it does not
replace their authorship. This independent fan-made project is not endorsed
by them or CD Projekt Red.

## What it does

A TCP client callable from CET Lua or REDscript. The original public API and
installation identity remain RedSocket, so existing clients can continue
using GetMod("RedSocket") and import RedSocket.*. Do not load the original
and this fork simultaneously. Each created socket has an asynchronous owner,
independent connection state, bounded command/event queues, and an update
callback path for native script delivery. It is not a server, assistant,
telemetry sensor or authenticated transport. It does not require a particular
receiving application.

- Sends CRLF-terminated single-line UTF-8 messages; receives LF or CRLF.
  Empty lines are valid. Embedded NUL, CR/LF and malformed UTF-8 are rejected.
  An invalid or oversized incoming line closes its own connection.
- Received payload limit: 1 MiB. Outbound pending write budget: 1 MiB
  including framing, with at most 512 pending writes per socket. A shared
  64 MiB accounted allocation budget covers admitted owner tasks and write
  payloads (including per-entry overhead); this is not a total process RSS
  bound. Queues can refuse new work; no application delivery is guaranteed.
- On orderly EOF, admitted complete lines from the just-closed epoch drain
  before its disconnect event; an incomplete tail is discarded. Other close
  paths can discard queued lines. Retired sockets never resume; stale
  completions cannot affect a replacement epoch.
- A successful send invocation means an offer to the transport, **not**
  delivery, application ACK or authentication. No automatic application
  retransmission. Keep correlation and expiry in your own protocol.
- No TLS or authentication. Use a trusted loopback server; any local process
  may impersonate a peer. The original TCP API permits other destinations;
  do not expose sensitive data or connect to untrusted hosts. Treat received
  commands as data, never as instructions to execute.
- Cleanup has a diagnostic wait, not a guaranteed hard deadline. Ordinary
  game shutdown was observed successfully on the tested pairing. Dynamic,
  forced or reentrant DLL unload is unsupported. Extended gameplay and
  resource soak have not been qualified.

## CET API

The module name stays RedSocket for compatibility. Example for use in your
own mod after Running starts, with a trusted local server listening:

```lua
local api = GetMod("RedSocket")
if api == nil then return end
local socket = api.createSocket()
if socket == nil then return end

local registered = socket:RegisterListener(
  function(line) -- handle untrusted input as data
    print("Received a line")
  end,
  function(status)
    if status == 0 then socket:SendCommand("hello", 1) end
  end,
  function() print("Disconnected") end,
  function() print("Send failed") end
)
if not registered then api.destroySocket(socket); return end
socket:Connect("127.0.0.1", 2077)

-- When your own mod no longer needs this socket:
-- api.destroySocket(socket)
```

Destroy only sockets you own. A destroyed wrapper is retired and cannot
reconnect; create a new one if needed. Destruction closes the socket, so
Disconnect is not required before Destroy. api.destroySocket returns false
for unknown/already retired wrappers. api.version identifies the CET wrapper
revision, not a cryptographic attestation of the loaded DLL. Do not mix
scripts and binaries from different releases. RegisterListener on the CET wrapper returns
whether the native listener registration succeeded. In REDscript, the legacy
void RegisterListener remains available; TryRegisterListener returns a
boolean, and IsRetired distinguishes an unusable socket. Do not overwrite
an existing listener while a connection or terminal callback is pending. The legacy optional
SendCommand limit argument is accepted for source compatibility; it no
longer requests a retry loop. The optional limit value also selects the wrapper's error-callback path;
without it no per-send error callback is registered. Error notification is
best-effort, not a guarantee that every rejected or abandoned frame produces an error callback.
Consumers must recover independently after disconnect, rejection or silence.

REDscript signatures remain in scripts/redscript/RedSocket.reds.
Use Socket.Create, not new Socket(); register listener callbacks before
Connect, and call Socket.Destroy when finished.

## Build and validation

See [RELEASE.md](RELEASE.md) for exact requirements, installation, rollback
and qualification boundaries; [CHANGELOG.md](CHANGELOG.md) for changes;
and [BUILD.md](BUILD.md) for pinned source dependencies and a separate
build directory. The build has no installer hook. Use the matching release
archive and manifest, never a mixture of bridge and DLL revisions.
No game assets, private application, or external runtime installers are
included. See [THIRD_PARTY.md](THIRD_PARTY.md) and the original notices in
licenses/.

## License

MIT, with the original copyright (c) 2025 Louis Poirier preserved, and
copyright (c) 2026 DeepBide for our modifications. See [LICENSE](LICENSE).
Other components retain their own licenses and authorship.
