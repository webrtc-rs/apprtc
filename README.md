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

AppRTC is a WebRTC reference application and signaling server in the `webrtc-rs` ecosystem. The current Rust
implementation provides the complete AppRTC-compatible P2P V1 room and signaling flow: HTTP join/message/leave APIs,
initiator election, two-member rooms, queued signaling messages, tokenless browser WebSocket registration, reconnect
grace, fallback POST/DELETE signaling, HTML templates, static web assets, ICE configuration, and HTTP/HTTPS plus WS/WSS
serving.

The current binaries support P2P V1. The repository also contains the Sans-I/O SFU implementation and the architecture
for P2P/SFU call modes, but V2 mode transitions and SFU worker integration are not yet enabled.

The Rust implementation replaces the previous unified Go Collider. The legacy implementation is retained only on the
repository's `go` branch.

## Architecture

The workspace has three Rust crates:

| Crate                    | Responsibility                                                                                                                                                  |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`apprtc`](apprtc)       | Standalone `appweb` and `signaling` binaries, CLI parsing, TLS listeners, logging, and graceful shutdown.                                                         |
| [`appweb`](appweb)       | AppRTC HTTP room API, configuration parameters, Jinja templates, static web assets, and the in-process client for the signaling authority.                      |
| [`signaling`](signaling) | Authoritative room/client state, V1 browser protocol, message queueing and relay, reconnect deadlines, Sans-I/O protocol layers, and the Axum WebSocket driver. |

AppWeb and signaling are separate processes and may run on different machines. AppWeb serves HTTP(S) and uses a
control WebSocket to submit `admit`, `remove`, `occupancy`, `inject`, and `status` operations to signaling. Browser
WebSocket traffic connects directly to signaling and never passes through AppWeb.

The signaling state is composed from Sans-I/O protocols:

```text
Collider
└── RoomTable
    └── Room
        └── Client
```

Tokio tasks own sockets and timers, while `Collider` owns deterministic signaling state. Successful V1 registration is
intentionally silent, and a disconnected registered client remains eligible to reconnect for 10 seconds before its
membership is removed.

## Current P2P V1 behavior

- Room and client IDs are opaque non-empty strings.
- A room contains at most two clients; a third join returns `FULL`.
- The first client is the initiator and the second is the callee.
- Removing a client promotes the survivor to initiator.
- Offers and trickle ICE candidates sent before the peer joins are queued.
- The second `/join` response returns queued messages in `params.messages`.
- The stock AppRTC asymmetric signaling flow is preserved: the initiator sends early signaling through `/message`, while
  the callee normally sends through WebSocket.
- A WebSocket disconnect starts a 10-second reconnect grace period instead of removing the client immediately.
- Root-path and `/_internal` POST/DELETE fallback routes are both supported.
- V1 identifiers are not restricted to numeric values. Numeric `u64` validation belongs to the future V2 protocol.

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
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 --tls &

# 2. Start AppWeb.
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url https://127.0.0.1:8080 --signaling-url wss://127.0.0.1:8081/ws \
  --signaling-insecure-tls --tls &

# 3. Run the integration tests.
cargo test -p apprtc --test '*' -- --nocapture

# 4. Stop both servers.
kill $(pgrep -f "target/debug/(appweb|signaling)") || true
```

CI performs the same sequence with a release build in `.github/workflows/tests.yml` and uploads the server log when the
job finishes.

## Run over HTTP and WebSocket

Run signaling and AppWeb separately from the repository root:

```bash
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url http://127.0.0.1:8080 --signaling-url ws://127.0.0.1:8081/ws
```

AppWeb prints:

```text
AppWeb listening on http://127.0.0.1:8080/
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) in a browser.

`--host-ip` and `--port` control each local listener. AppWeb's `--public-url` controls the browser-facing HTTP origin;
`--signaling-url` controls the browser-facing WebSocket origin and must include `/ws`.

## Run over HTTPS and secure WebSocket

Add `--tls` to serve real HTTPS and WSS from the same listener:

```bash
cargo run -p apprtc --bin signaling -- \
  --host-ip 127.0.0.1 --port 8081 --tls
cargo run -p apprtc --bin appweb -- \
  --host-ip 127.0.0.1 \
  --port 8080 \
  --web-root appweb \
  --public-url https://127.0.0.1:8080 \
  --signaling-url wss://127.0.0.1:8081/ws \
  --signaling-insecure-tls \
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
  --host-ip 0.0.0.0 --public-url wss://sfu.example.com --port 443 --tls \
  --certificate /path/to/fullchain.pem \
  --private-key /path/to/privkey.pem

cargo run -p apprtc --bin appweb -- \
  --host-ip 0.0.0.0 --public-url https://apprtc.example.com --port 443 --web-root appweb \
  --signaling-url wss://sfu.example.com/ws --tls \
  --certificate /path/to/fullchain.pem --private-key /path/to/privkey.pem
```

## Command-line options

Run `cargo run -p apprtc --bin appweb -- --help` or `cargo run -p apprtc --bin signaling -- --help` for the
authoritative lists.

| Option | Default | Description |
|---|---:|---|
| `--host-ip <HOST-IP>` | `127.0.0.1` | Local listener bind address (both binaries). |
| `--public-url <URL>` | listener address/scheme | Browser-facing HTTP(S) origin (`appweb`) or WS(S) origin (`signaling`). |
| `-p, --port <PORT>` | `8080`/`8081` | AppWeb HTTP(S) or signaling WS(S) listening port. |
| `--web-root <PATH>` | `appweb` | Static asset directory (`appweb`). |
| `--tls` | off | Serve HTTPS/WSS instead of HTTP/WS. |
| `--certificate <PATH>` | bundled certificate | PEM certificate chain used with `--tls`. |
| `--private-key <PATH>` | bundled key | PEM private key used with `--tls`. |
| `--signaling-url <URL>` | none | Signaling control/browser WebSocket URL, including `/ws` (`appweb`). |
| `--signaling-insecure-tls` | off | Disable verification for local self-signed signaling TLS (`appweb`). |
| `--appid`, `--signaling-token` | `appweb-1`, empty | AppWeb control identity and token (`appweb`). |
| `--ice-server-url <URLS>` | empty | ICE server URLs (`appweb`). |
| `--ice-server-base-url <URL>` | AppWeb origin | External ICE credential service origin (`appweb`). |
| `--ice-server-api-key <KEY>` | empty | API key for the ICE credential service (`appweb`). |
| `--header-message <TEXT>` | empty | Banner displayed by the web application (`appweb`). |
| `--bypass-join-confirmation` | off | Skip the browser ready-to-join prompt (`appweb`). |
| `-d, --debug` | off | Enable application logging (both binaries). |
| `-l, --level <LEVEL>` | `info` | Log filter (both binaries). |
| `-o, --output-log-file <PATH>` | stdout | Write formatted logs to a file (both binaries). |

Example ICE configuration:

```bash
cargo run -p apprtc -- \
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

## Repository layout

```text
apprtc/       Rust executable crate and development TLS certificate
appweb/       Rust web/API crate plus AppRTC browser assets
signaling/    Rust Sans-I/O signaling authority and WebSocket driver
sfu/          Rust Sans-I/O selective-forwarding media server
docs/design/  Signaling architecture and protocol design
```

## License

This project is dual-licensed under the MIT and Apache-2.0 licenses.

## Contributing

Contributors and pull requests are welcome.
