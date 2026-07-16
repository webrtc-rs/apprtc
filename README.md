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

[AppRTC](https://appr.tc) is a WebRTC-rs reference application designed for peer-to-peer (P2P) and selective forwarding unit (SFU) calling and signaling.

The public instance is online at **[https://appr.tc](https://appr.tc)**.

Currently, this signaling server is implemented in **Go (Golang)**, but it will be **rewritten in Rust soon** as part of
the `webrtc-rs` ecosystem.

> [!NOTE]
> The majority of the Go code in the [collider](go/collider) directory and the client-side assets in
> the [web_app](go/web_app) directory are based on the deprecated Google AppRTC reference project located
> at [https://github.com/webrtc/apprtc](https://github.com/webrtc/apprtc).
>
> **Key Modifications:**
> * We have **completely removed the Python codebase and Google App Engine (GAE) dependencies**.
> * The Python-based Room Server was consolidated directly into the Go `collider` application. The unified Go server now
    handles both room matching/metadata (HTTP APIs) and WebSocket message relaying (old collider) in a single process.

---

## AppRTC P2P/SFU Signaling Protocol

The AppRTC signaling process consists of an initial HTTP room handshaking API and a WebSocket-based messaging protocol.

### 1. HTTP Room API

Clients interact with the room server using the following HTTP endpoints:

* **`POST /join/{roomid}`**
    * Joins a room.
    * **Response**: Returns a JSON object containing the assigned `client_id`, room occupancy status (`is_initiator`),
      and WebRTC/ICE configuration parameters.
* **`POST /message/{roomid}/{clientid}`**
    * Sends/injects a signaling message (such as SDP or ICE candidates) to the other client in the room. Often used as a
      fallback if WebSocket connection is not established yet.
* **`POST /leave/{roomid}/{clientid}`**
    * Notifies the server that the client is leaving the room. Cleans up the room state.
* **`GET /params`**
    * Returns global, room-independent configuration parameters.
* **`GET /v1alpha/iceconfig`**
    * Retrieves the list of STUN/TURN servers used for ICE candidates.

---

### 2. WebSocket Signaling Protocol (`/ws`)

Once joined, clients establish a persistent WebSocket connection to `/ws` for bi-directional signaling. The protocol
supports the following JSON commands:

#### A. Client-to-Server Commands

* **`register`**
    * Sent by the client immediately after the WebSocket connection opens to bind the socket to a room and client ID.
    * **Format**:
      ```json
      {
        "cmd": "register",
        "roomid": "<ROOM_ID>",
        "clientid": "<CLIENT_ID>"
      }
      ```

* **`send`**
    * Sent by a registered client to forward an arbitrary signaling payload to the peer.
    * **Format**:
      ```json
      {
        "cmd": "send",
        "msg": "<JSON_STRING_PAYLOAD>"
      }
      ```
      *Note: The `msg` value is a stringified JSON object containing the actual WebRTC signaling payload (see below).*

#### B. Server-to-Client Messages

* **Signaling Relay**:
    * Forwarded messages from a peer client.
    * **Format**:
      ```json
      {
        "msg": "<JSON_STRING_PAYLOAD>"
      }
      ```

* **Error Message**:
    * Sent by the server when a WebSocket command fails (e.g., invalid command sequence or duplicate registrations).
    * **Format**:
      ```json
      {
        "error": "<ERROR_DESCRIPTION>"
      }
      ```

---

### 3. Detailed Signaling Payloads (`msg` Object)

The `msg` field (in both `cmd: "send"` and the server relay message) contains a serialized JSON string representing one
of the following WebRTC signaling events:

#### A. SDP Offer

Sent by the initiator to propose a connection configuration.

```json
{
  "type": "offer",
  "sdp": "v=0\r\no=- 4611731400430051336 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\n..."
}
```

#### B. SDP Answer

Sent by the receiver in response to the initiator's offer.

```json
{
  "type": "answer",
  "sdp": "v=0\r\no=- 1234567890123456789 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\n..."
}
```

#### C. ICE Candidate

Sent incrementally by either peer as network routing options are discovered.

```json
{
  "type": "candidate",
  "label": 0,
  "id": "sdpMid",
  "candidate": "candidate:842163049 1 udp 16777215 192.168.1.100 54321 typ host generation 0 ufrag A1B2 ..."
}
```

#### D. Bye (Hang up)

Sent when a peer disconnects or leaves the call.

```json
{
  "type": "bye"
}
```

---

## Deployment

For details on compiling, running, and deploying this server to Fedora or other Linux environments using Let's Encrypt
certificates, see [deployment/README.md](./deployment/README.md).


## Building

### Toolchain

Use a Rust toolchain with Edition 2024 support.

### Build & test

```bash
# Fetch the submodules first
git submodule update --init --recursive
cargo clippy
cargo fmt
cargo build
cargo test
```

## Open Source License

This project uses dual licensing under MIT or Apache-2.0.

## Contributing

Contributors and pull requests are welcome.
