<h1 align="center">
 <a href="https://webrtc.rs"><img src="https://raw.githubusercontent.com/webrtc-rs/webrtc-rs.github.io/master/res/apprtc.png" alt="WebRTC.rs"></a>
 <br>
</h1>
<p align="center">
 <a href="https://github.com/webrtc-rs/apprtc/actions">
  <img src="https://github.com/webrtc-rs/apprtc/workflows/cargo/badge.svg">
 </a>
 <a href="https://codecov.io/gh/webrtc-rs/apprtc">
  <img src="https://codecov.io/gh/webrtc-rs/apprtc/branch/master/graph/badge.svg">
 </a>
 <a href="https://deps.rs/repo/github/webrtc-rs/apprtc">
  <img src="https://deps.rs/repo/github/webrtc-rs/apprtc/status.svg">
 </a>
 <a href="https://crates.io/crates/apprtc">
  <img src="https://img.shields.io/crates/v/apprtc.svg">
 </a>
 <a href="https://docs.rs/apprtc">
  <img src="https://docs.rs/apprtc/badge.svg">
 </a>
 <a href="https://doc.rust-lang.org/1.6.0/complement-project-faq.html#why-dual-mitasl2-license">
  <img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License: MIT/Apache 2.0">
 </a>
 <a href="https://discord.gg/4Ju8UHdXMs">
  <img src="https://img.shields.io/discord/800204819540869120?logo=discord" alt="Discord">
 </a>
 <a href="https://twitter.com/WebRTCrs">
  <img src="https://img.shields.io/twitter/url/https/twitter.com/webrtcrs.svg?style=social&label=%40WebRTCrs" alt="Twitter">
 </a>
</p>
<p align="center">
 <strong>AppRTC P2P/SFU Signaling Server in Rust</strong>
</p>

AppRTC is a WebRTC reference application and signaling server in the `webrtc-rs` ecosystem. The Rust implementation
supports the AppRTC-compatible P2P V1 flow and the token-authenticated V2 P2P/SFU flow. The room-selection page defaults
to V2 with a checked **V2 P2P/SFU** checkbox; unchecking it falls back to the legacy V1 flow. V2 uses numeric `u64` room/client IDs, namespaced HTTP routes,
signaling-issued admission tokens, explicit WebSocket registration acknowledgement, signal epochs, symmetric WebSocket
offer/answer/trickle-ICE relay, reconnect grace, and survivor promotion.

The first two V2 members use a direct P2P connection. When a third member joins, signaling selects a ready SFU worker with sufficient advertised capacity, waits for all three worker-side joins, commits a new signal epoch, and tells the existing browsers to create fresh SFU peer connections while their P2P connection remains active. The third browser joins directly in SFU mode. Browsers use the polite-peer perfect-negotiation path against authoritative SFU subscribe offers and republish after an offer collision. SFU→P2P downgrade is not implemented yet.

The Rust implementation replaces the previous unified Go Collider. The legacy implementation is retained only on the
repository's `go` branch.

## Architecture

The workspace has four Rust crates:

| Crate                                | Responsibility                                                                                                                                                                                                                 |
|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`apprtc`](apprtc)                   | Standalone `appweb`, `signaling`, and `sfu` binaries plus their runtime adapters: CLI parsing, TLS listeners, logging, graceful shutdown, browser WebSocket I/O, private gRPC, UDP media I/O, and the Sans-I/O SFU driver.     |
| [`appweb`](appweb)                   | AppRTC HTTP room API, configuration parameters, Jinja templates, static web assets, and a reusable gRPC client for the signaling authority.                                                                                    |
| [`signaling`](signaling)             | Authoritative V1 and V2 P2P/SFU room, client, worker, lifecycle, transition, browser-protocol, token/epoch, replay, and reconnect state — a pure Sans-I/O crate with no sockets, threads, clock, or entropy source of its own. |
| [`signaling-proto`](signaling-proto) | Generated Protobuf and tonic contract shared by AppWeb, signaling, and SFU workers.                                                                                                                                            |

AppWeb, signaling, and SFU are separate processes and may run on different machines. AppWeb serves HTTP(S) and uses
concurrent unary gRPC calls over one reusable HTTP/2 channel to submit V1 and V2 admission, removal, occupancy, V1
injection, and status operations to signaling. Browser WebSocket traffic connects directly to signaling and never passes
through AppWeb. Each SFU process owns one reconnecting bidirectional `OpenSfuSession` gRPC stream to signaling; browser
media travels directly to the SFU over ICE/DTLS/SRTP and never passes through AppWeb or signaling.

The signaling state is composed from Sans-I/O protocols:

```text
Collider
└── RoomTable
    └── Room
        └── Client
```

Every layer implements the `sansio::Protocol` trait, so the whole signaling state machine is deterministic and testable
in memory, without sockets or a wall clock.

The `apprtc` library keeps each runtime responsibility in a dedicated module:

```text
apprtc/src/
├── grpc_server.rs        private signaling gRPC service adapter
├── sfu_server.rs         signaling stream, UDP media shards, and Sans-I/O SFU adapter
├── signaling_server.rs   command channel and single-owner Collider event loop
└── ws_server.rs          public browser TCP/TLS, HTTP upgrade, and WebSocket sessions
```

[`apprtc/src/ws_server.rs`](apprtc/src/ws_server.rs) accepts browser `/ws` connections and converts WebSocket lifecycle
events and text frames into driver commands. [`apprtc/src/grpc_server.rs`](apprtc/src/grpc_server.rs) adapts private
gRPC requests to the same command channel. [`apprtc/src/signaling_server.rs`](apprtc/src/signaling_server.rs) owns the
single Collider event loop, serializes every browser and authority operation, fires protocol timeouts, and routes
outputs back to the WebSocket or gRPC caller. Tasks sleep on async I/O, deadlines, or shutdown without polling.

Successful V1 registration is intentionally silent, and a disconnected registered client remains eligible to
reconnect for 10 seconds before its membership is removed. The reconnect grace applies to P2P (V1 and V2) members
only; an SFU member whose WebSocket drops is treated as an immediate leave so its forwarded media stops and the
other participants' grid tiles are removed without delay.

## Current P2P behavior

V1 preserves the legacy AppRTC contract:

- Room and client IDs are opaque non-empty strings.
- A room contains at most two clients; a third join returns `FULL`.
- The first client is the initiator and the second is the callee.
- Removing a client promotes the survivor to initiator.
- Offers and trickle ICE candidates sent before the peer joins are queued.
- The second `/join` response returns queued messages in `params.messages`.
- The stock AppRTC asymmetric signaling flow is preserved: the initiator sends early signaling through `/message`, while
  the callee normally sends through WebSocket.
- A WebSocket disconnect starts a 10-second reconnect grace period instead of removing the client immediately (P2P only; SFU members leave immediately on disconnect).
- Root-path and `/_internal` POST/DELETE fallback routes are both supported.
- V1 identifiers are not restricted to numeric values.

P2P V2 adds:

- A checked **V2 P2P/SFU** room-selection checkbox by default; unchecking it selects the legacy V1 flow.
- `/v2/r/{roomid}`, `/v2/join/{roomid}`, `/v2/leave/{roomid}/{clientid}`, and `/v2/params` routes.
- Canonical decimal `u64` room and client IDs.
- A signaling-issued admission token bound to the room/client pair.
- Explicit `{control:"registered"}` acknowledgement before signaling starts.
- An `epoch` on every browser `send` frame; stale or malformed epochs are dropped.
- Symmetric WebSocket relay for offers, answers, and trickle-ICE candidates; V2 does not use `/message` or the V1
  WebSocket POST fallback.
- Authenticated leave using `Authorization: Bearer <admission_token>` and `p2p-promote` for the surviving participant.

SFU-capable V2 adds:

- Capacity-aware selection of a ready SFU worker when the third member joins. Eligible workers are ordered by assigned clients, then assigned rooms, then `instance_id`; the room remains pinned to the selected worker.
- An ordered `JoinMember` barrier for all room members before signaling commits `Upgrading` to `SFU` and increments the
  signal epoch.
- A fresh browser SFU peer connection while the existing P2P connection remains active; the old P2P connection closes
  only after SFU ICE connects.
- Grid-based remote participants: the peer video/audio fills the window as a responsive grid of per-publisher tiles, the
  only UI change from P2P — the self-view and call controls keep their P2P positions. Each tile groups a peer's
  forwarded video and audio, and a peer's tile is removed from the grid when it leaves the room (reconciled from the
  negotiated transceivers after each SFU re-offer).
- SFU publish/subscribe SDP and full trickle-ICE exchange through the same browser V2 WebSocket envelope.
- Reliable worker events, command result correlation and deduplication, health/capacity reporting, same-instance
  reconnect synchronization, and command replay.
- Ordered worker-side joins and leaves for members admitted to or removed from an existing SFU room.
- V2 perfect negotiation in the browser: it is polite toward the SFU, rolls back a colliding local offer, answers the SFU offer with its `requestid`, and creates a fresh publish offer afterward.

SFU→P2P downgrade and cross-worker migration of an established room are intentionally deferred. A disconnected worker may resume its rooms only by reconnecting with the same process-incarnation `instance_id` during the recovery grace period; otherwise signaling fails those rooms with `room-failed` rather than moving live WebRTC transports.

## Current limitations

- The JavaScript `SignalingChannel` does not yet automatically reconnect a closed browser WebSocket. The signaling authority preserves P2P membership during its 10-second grace, but exploiting that grace currently requires a new registration attempt by the client.
- An SFU-member WebSocket disconnect intentionally initiates immediate member leave rather than browser reconnect grace.
- A committed room whose SFU worker is lost becomes `Failed` and emits `room-failed`; automatic failed-room cleanup and transparent browser rejoin are not implemented.
- Advertised worker capacity is enforced when selecting a worker for the initial three-member upgrade. Later joins remain pinned to that worker and are serialized through `JoinMember`, but the authority does not currently pre-check `max_clients` again.
- Service gRPC supports server-authenticated TLS but not mTLS/client authentication. Restrict the gRPC listener to trusted hosts.

## Build and test

Use a Rust toolchain with Edition 2024 support.

```bash
git submodule update --init --recursive
cargo build --workspace
cargo test --workspace --lib --bins
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

The integration tests are black-box clients of real standalone AppWeb and signaling TLS servers:

```bash
# 1. Start signaling.
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 \
  --grpc-port 50051 --tls &

# 2. Start one SFU worker.
cargo run -p apprtc --bin sfu -- --host-ip 127.0.0.1 \
  --media-port-min 35000 --media-port-max 35000 \
  --grpc-url https://127.0.0.1:50051 --insecure-tls &

# 3. Start AppWeb.
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url https://127.0.0.1:8080 --ws-url wss://127.0.0.1:8081/ws \
  --grpc-url https://127.0.0.1:50051 --insecure-tls --tls &

# 4. Run the integration tests.
cargo test -p apprtc --test '*' -- --nocapture

# 5. Stop all three services.
kill $(pgrep -f "target/debug/(appweb|signaling|sfu)") || true
```

CI performs the same sequence with a release build in `.github/workflows/tests.yml` and uploads the service logs when the job finishes. The black-box suite covers V1 compatibility, V2 P2P relay, the real AppWeb→signaling→SFU third-member join barrier, three-client SFU data channels, and RTP/RTCP media forwarding.

## Run over HTTP and WebSocket

Run signaling, one SFU worker, and AppWeb separately from the repository root:

```bash
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 \
  --grpc-port 50051
cargo run -p apprtc --bin sfu -- --host-ip 127.0.0.1 \
  --media-port-min 35000 --media-port-max 35000 \
  --grpc-url http://127.0.0.1:50051
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url http://127.0.0.1:8080 --ws-url ws://127.0.0.1:8081/ws \
  --grpc-url http://127.0.0.1:50051
```

AppWeb prints:

```text
AppWeb listening on http://127.0.0.1:8080/
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) in a browser.

`--host-ip` controls the bind address for each process. In the signaling process it applies to both the browser
WebSocket listener and the private gRPC listener; `--port` and `--grpc-port` select their respective ports. AppWeb's
`--public-url` controls the browser-facing HTTP origin, `--ws-url` controls the browser-facing WebSocket URL and must
include `/ws`, and `--grpc-url` independently selects the private signaling gRPC origin.

## Run over HTTPS and secure WebSocket

Add `--tls` to serve real HTTPS and WSS from the same listener:

```bash
cargo run -p apprtc --bin signaling -- \
  --host-ip 127.0.0.1 --port 8081 --tls \
  --grpc-port 50051
cargo run -p apprtc --bin sfu -- \
  --host-ip 127.0.0.1 \
  --media-port-min 35000 --media-port-max 35000 \
  --grpc-url https://127.0.0.1:50051 --insecure-tls
cargo run -p apprtc --bin appweb -- \
  --host-ip 127.0.0.1 \
  --port 8080 \
  --web-root appweb \
  --public-url https://127.0.0.1:8080 \
  --ws-url wss://127.0.0.1:8081/ws \
  --grpc-url https://127.0.0.1:50051 \
  --insecure-tls \
  --tls \
  --debug \
  --level info
```

Without certificate options, AppRTC uses the bundled development certificate at [
`apprtc/cert/cert.pem`](apprtc/cert/cert.pem). Its subject alternative names include `localhost`, `127.0.0.1`, and
`::1`, but it is self-signed. Trust that certificate in the browser or operating-system trust store before opening the
page; otherwise HTTPS and WSS clients will reject it with `CertificateUnknown` or an equivalent certificate-authority
error.

For a deployment, supply a certificate issued by a trusted authority. Both options must be supplied together:

```bash
cargo run -p apprtc --bin signaling -- \
  --host-ip 0.0.0.0 --port 443 --tls \
  --grpc-port 50051 \
  --certificate /path/to/fullchain.pem \
  --private-key /path/to/privkey.pem

cargo run -p apprtc --bin sfu -- \
  --host-ip 0.0.0.0 --media-public-ip 203.0.113.20 \
  --media-port-min 3478 --media-port-max 3497 \
  --grpc-url https://sfu.example.com:50051

cargo run -p apprtc --bin appweb -- \
  --host-ip 0.0.0.0 --public-url https://apprtc.example.com --port 443 --web-root appweb \
  --ws-url wss://sfu.example.com/ws --grpc-url https://sfu.example.com:50051 --tls \
  --certificate /path/to/fullchain.pem --private-key /path/to/privkey.pem
```

## Command-line options

Run `cargo run -p apprtc --bin appweb -- --help`, `cargo run -p apprtc --bin signaling -- --help`, or
`cargo run -p apprtc --bin sfu -- --help` for the authoritative lists.

| Option                         |                  Default | Description                                                                                      |
|--------------------------------|-------------------------:|--------------------------------------------------------------------------------------------------|
| `--host-ip <HOST-IP>`          |              `127.0.0.1` | Local TCP or UDP bind address (all binaries).                                                    |
| `--public-url <URL>`           |                     none | Required browser-facing HTTP(S) origin (`appweb`).                                               |
| `-p, --port <PORT>`            |            `8080`/`8081` | AppWeb HTTP(S), signaling WS(S), or SFU redirect (`--redirect-url`) port.                        |
| `--web-root <PATH>`            |                 `appweb` | Static asset directory (`appweb`).                                                               |
| `--tls`                        |                      off | Serve AppWeb HTTPS, both signaling TLS listeners, or the optional SFU HTTPS redirect.             |
| `--certificate <PATH>`         |      bundled certificate | PEM certificate chain used with `--tls` by the relevant listener.                                |
| `--private-key <PATH>`         |              bundled key | PEM private key used with `--tls` by the relevant listener.                                      |
| `--ws-url <URL>`               |                     none | Public browser signaling WebSocket URL ending in `/ws` (`appweb`).                               |
| `--grpc-url <URL>`             | `http://127.0.0.1:50051` | Private signaling gRPC origin (`appweb` and `sfu`).                                              |
| `--insecure-tls`               |                      off | Disable gRPC verification for local self-signed TLS (`appweb`, `sfu`).                           |
| `--grpc-port <PORT>`           |                  `50051` | Private gRPC listener port (`signaling`).                                                        |
| `--ice-server-url <URLS>`      |                    empty | ICE server URLs (`appweb`).                                                                      |
| `--ice-server-base-url <URL>`  |            AppWeb origin | External ICE credential service origin (`appweb`).                                               |
| `--ice-server-api-key <KEY>`   |                    empty | API key for the ICE credential service (`appweb`).                                               |
| `--header-message <TEXT>`      |                    empty | Banner displayed by the web application (`appweb`).                                              |
| `--bypass-join-confirmation`   |                      off | Skip the browser ready-to-join prompt (`appweb`).                                                |
| `--media-public-ip <IP>`       |              `--host-ip` | ICE candidate address advertised by `sfu`; set only when it differs from the bind address (NAT). |
| `--redirect-url <URL>`         |                    empty | When set, `sfu` runs a server on `--host-ip:--port` that redirects every request here.           |
| `--media-port-min <PORT>`      |                   `3478` | First UDP media port owned by `sfu`.                                                             |
| `--media-port-max <PORT>`      |                   `3497` | Last UDP media port owned by `sfu`.                                                              |
| `--max-rooms <COUNT>`          |                   `1000` | SFU room capacity advertised to signaling.                                                       |
| `--max-clients <COUNT>`        |                  `10000` | SFU client capacity advertised to signaling.                                                     |
| `--instance-id <ID>`           |          generated value | Optional SFU process-incarnation ID; normally omit it.                                           |
| `-d, --debug`                  |                      off | Enable application logging (all binaries).                                                       |
| `-l, --level <LEVEL>`          |                   `info` | Log filter (all binaries).                                                                       |
| `-o, --output-log-file <PATH>` |                   stdout | Write formatted logs to a file (all binaries).                                                   |

Example ICE configuration:

```bash
cargo run -p apprtc --bin appweb -- \
  --ice-server-url stun:stun.l.google.com:19302 \
  --ice-server-url turn:turn.example.com:3478
```

## HTTP API

| Method and path                                   | Behavior                                                                               |
|---------------------------------------------------|----------------------------------------------------------------------------------------|
| `GET /`                                           | Render the room-selection page.                                                        |
| `GET /r/{roomid}`                                 | Render the call page or the full-room page when occupancy is two.                      |
| `POST /join/{roomid}`                             | Generate an eight-digit client ID, admit it, and return legacy AppRTC room parameters. |
| `POST /message/{roomid}/{clientid}`               | Queue or relay the raw signaling message and return `{ "result": "SUCCESS" }`.         |
| `POST /leave/{roomid}/{clientid}`                 | Remove the client and return the legacy empty success response.                        |
| `GET /params`                                     | Return room-independent AppRTC parameters.                                             |
| `GET` or `POST /v1alpha/iceconfig`                | Return the configured ICE server list.                                                 |
| `GET /status`                                     | Return uptime and WebSocket/HTTP counters.                                             |
| `POST /{roomid}/{clientid}`                       | V1 `wss_post_url` fallback: inject a raw signaling message.                            |
| `DELETE /{roomid}/{clientid}`                     | V1 `wss_post_url` fallback: remove the client.                                         |
| `POST` or `DELETE /_internal/{roomid}/{clientid}` | Compatibility alias for the fallback bridge.                                           |
| `GET /v2/r/{roomid}`                              | Render the V2 P2P/SFU call page; `roomid` must be canonical decimal `u64`.              |
| `POST /v2/join/{roomid}`                           | Admit a V2 member; a third member initiates the SFU join barrier.                       |
| `POST /v2/leave/{roomid}/{clientid}`               | Remove a V2 member using its bearer admission token.                                   |
| `GET /v2/params`                                   | Return room-independent V2 parameters and ICE configuration.                           |

Static files under `appweb/js`, `appweb/css`, `appweb/images`, and `appweb/html` are served by the same process.

### Join response

A successful join returns the legacy shape consumed by `appweb/js/call.js`:

```json
{
  "result": "SUCCESS",
  "params": {
    "room_id": "example-room",
    "client_id": "12345678",
    "is_initiator": "true",
    "wss_url": "ws://127.0.0.1:8081/ws",
    "wss_post_url": "http://127.0.0.1:8080"
  }
}
```

The actual `params` object also contains peer-connection constraints, ICE configuration, media constraints, room links,
loopback settings, and UI configuration.

## V1 WebSocket protocol

Browsers connect to `/ws` and send a registration frame first:

```json
{
  "cmd": "register",
  "roomid": "example-room",
  "clientid": "12345678"
}
```

A successful V1 registration has no acknowledgement. The registered client sends an opaque signaling payload with:

```json
{
  "cmd": "send",
  "msg": "{\"type\":\"candidate\",\"label\":0,\"id\":\"0\",\"candidate\":\"candidate:...\"}"
}
```

The peer receives the payload without the signaling authority parsing or modifying the inner JSON:

```json
{
  "msg": "{\"type\":\"candidate\",\"label\":0,\"id\":\"0\",\"candidate\":\"candidate:...\"}",
  "error": ""
}
```

Protocol errors are sent once before the socket is closed:

```json
{
  "msg": "",
  "error": "Client not registered"
}
```

The inner `msg` string may contain an SDP offer, SDP answer, trickle ICE candidate, end-of-candidates marker, or `bye`
object. V1 signaling treats it as an opaque UTF-8 string.

## V2 WebSocket protocol

V2 uses the same `/ws` endpoint but requires numeric identifiers, an admission token, and `ver: 2`:

```json
{
  "cmd": "register",
  "roomid": "42",
  "clientid": "101",
  "ver": 2,
  "token": "admission-token"
}
```

Successful registration returns an authoritative snapshot before any queued signaling:

```json
{
  "control": "registered",
  "roomid": "42",
  "epoch": "0",
  "mode": "p2p",
  "is_initiator": true
}
```

Every V2 signaling message carries the current epoch. The inner `msg` remains an opaque JSON string and fully supports SDP, trickle ICE, and end-of-candidates:

```json
{
  "cmd": "send",
  "epoch": "0",
  "msg": "{\"type\":\"candidate\",\"label\":0,\"id\":\"0\",\"candidate\":\"candidate:...\"}"
}
```

The server may send `p2p-promote`, `sfu-upgrade`, and `room-failed` controls. In SFU mode, subscribe offers contain a decimal-string `requestid`; the browser echoes it in the corresponding answer.

## Multiple SFU workers

Each SFU process opens one `OpenSfuSession` stream and must have a unique process-incarnation `instance_id` (the binary generates one when `--instance-id` is omitted). Signaling considers only connected workers whose latest health state is `Ready` and whose advertised room/client capacity can accept the initial three-member assignment.

Among eligible workers, signaling chooses the lowest tuple `(assigned_clients, assigned_rooms, instance_id)`. This is least-loaded placement, not round-robin. Once selected, every member and all signaling/media state for that room remain affine to that worker. Later rooms can be placed on other workers, but an established room is not split or automatically migrated.

## Status endpoint

`GET /status` preserves the Collider-compatible response:

```json
{
  "upsec": 12.5,
  "openws": 2,
  "totalws": 5,
  "wserrors": 0,
  "httperrors": 0
}
```

## License

This project is dual-licensed under the MIT and Apache-2.0 licenses.

## Contributing

Contributors and pull requests are welcome.
