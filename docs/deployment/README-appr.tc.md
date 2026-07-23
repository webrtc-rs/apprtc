# AppRTC Deployment - appr.tc

This guide deploys Rust AppRTC as three services on one host. AppWeb serves the browser application and HTTP room APIs.
Signaling owns V1 and V2 room state, serves the public browser WebSocket endpoint, and exposes a private gRPC listener
to AppWeb and the SFU. The SFU owns the UDP media ports and maintains one bidirectional gRPC session to signaling.

```text
Browser ── HTTPS ──> AppWeb (https://appr.tc:443)
Browser ── WSS ────> Signaling (wss://appr.tc:8443/ws)
AppWeb  ── gRPC/HTTP2/TLS ──> Signaling (https://appr.tc:50051)
SFU 1   ── gRPC/HTTP2/TLS ──> Signaling (https://appr.tc:50051)
Browser <── ICE/DTLS/SRTP over UDP ──> SFU (appr.tc:3478-3497)
```

V1 remains backward compatible. In V2, the first two participants use P2P; a third participant triggers P2P→SFU upgrade.
SFU→P2P downgrade is not implemented yet.

## DNS and firewall

Point `appr.tc` at the host. Allow TCP `443` for AppWeb, TCP `8443` for signaling, and UDP `3478-3497` for SFU media.
Port `80` is only needed for Certbot standalone validation. Keep TCP `50051` blocked from the public Internet.

* **A Record** pointing `@` to the server IP (e.g., `173.249.199.192`)
* **A Record** pointing `www` to the same server if required
* **Note**: Make sure to delete any conflicting CNAME or AAAA (IPv6) records.

## Install prerequisites

On Fedora:

```bash
sudo dnf install -y git rsync certbot rust cargo
```

Use the current stable Rust toolchain if the distribution version does not support Edition 2024. Or

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## TLS certificates

Obtain a certificate for `appr.tc`:

```bash
sudo certbot certonly --standalone -d appr.tc -d www.appr.tc --agree-tos
```

The binaries accept the certificate and key directly:

```text
/etc/letsencrypt/live/appr.tc/fullchain.pem
/etc/letsencrypt/live/appr.tc/privkey.pem
```

## Install and build

Copy the repository to the host:

```bash
rsync -avz --exclude target --exclude .git --exclude .idea ./ root@173.249.199.192:/opt/apprtc/
```

Build the required binaries:

```bash
cd /opt/apprtc
cargo build --release -p apprtc --bin appweb --bin signaling --bin sfu
chmod +x /opt/apprtc/target/release/appweb /opt/apprtc/target/release/signaling /opt/apprtc/target/release/sfu
```

For an upgrade after the systemd units below have already been installed, restart both services with:

```bash
sudo systemctl restart apprtc-signaling apprtc-sfu apprtc-appweb
```

## Production services

Run signaling on the same host, bound to port `8443`:

```bash
nano /etc/systemd/system/apprtc-signaling.service
```

```ini
[Unit]
Description=AppRTC Signaling WebSocket and gRPC server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStartPre=/bin/sh -c 'mkdir -p /opt/logs; if [ -f /opt/logs/signaling.log ]; then mv /opt/logs/signaling.log /opt/logs/signaling-$(date +%%Y%%m%%d-%%H%%M%%S).log; fi'
ExecStart=/opt/apprtc/target/release/signaling --host-ip 0.0.0.0 --port 8443 --grpc-port 50051 --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/signaling.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Run AppWeb on the same host on port `443`:

```bash
nano /etc/systemd/system/apprtc-appweb.service
```

```ini
[Unit]
Description=AppRTC AppWeb HTTP server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStartPre=/bin/sh -c 'mkdir -p /opt/logs; if [ -f /opt/logs/appweb.log ]; then mv /opt/logs/appweb.log /opt/logs/appweb-$(date +%%Y%%m%%d-%%H%%M%%S).log; fi'
ExecStart=/opt/apprtc/target/release/appweb --host-ip 0.0.0.0 --public-url https://appr.tc --ws-url wss://appr.tc:8443/ws --grpc-url https://appr.tc:50051 --port 443 --web-root /opt/apprtc/appweb --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/appweb.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Run the SFU on the same host and advertise the host's public IP address:

```bash
nano /etc/systemd/system/apprtc-sfu.service
```

```ini
[Unit]
Description=AppRTC SFU media worker
After=network-online.target apprtc-signaling.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStartPre=/bin/sh -c 'mkdir -p /opt/logs; if [ -f /opt/logs/sfu.log ]; then mv /opt/logs/sfu.log /opt/logs/sfu-$(date +%%Y%%m%%d-%%H%%M%%S).log; fi'
ExecStart=/opt/apprtc/target/release/sfu --host-ip 0.0.0.0 --media-public-ip 173.249.199.192 --media-port-min 3478 --media-port-max 3497 --grpc-url https://appr.tc:50051 -d -l info -o /opt/logs/sfu.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Replace `173.249.199.192` with the host's actual public IP. `--host-ip` controls UDP binding; `--media-public-ip` is placed in ICE candidates and therefore must be reachable by browsers. It defaults to `--host-ip`, so set it when binding a wildcard/private address but advertising a public address. This unit does not enable the SFU binary's optional HTTP redirect server; consequently it does not need `--port`, `--redirect-url`, `--tls`, `--certificate`, or `--private-key`. The SFU gRPC client selects server-authenticated TLS through its `https://` URL.

The signaling `--tls` flag protects both its WebSocket and gRPC listeners with the `appr.tc` certificate. The gRPC listener binds `0.0.0.0` so local clients can connect with the certificate-valid hostname `https://appr.tc:50051`; keep TCP `50051` blocked by the host/provider firewalls while every client runs on this host. Enable all three services:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-signaling
sudo systemctl enable --now apprtc-sfu
sudo systemctl enable --now apprtc-appweb
sudo systemctl status apprtc-signaling apprtc-sfu apprtc-appweb
```

The services handle SIGINT gracefully by draining HTTP/gRPC requests, closing WebSocket connections, closing SFU peer connections, and releasing signaling/media state.

## Add another SFU worker

Signaling automatically selects among every connected, ready worker. It filters by advertised room/client capacity and chooses the worker with the fewest assigned clients, then the fewest assigned rooms, then the lexicographically smallest `instance_id`. A room is pinned to one worker for its lifetime; this is placement load balancing, not per-participant distribution or live room migration.

Two workers on the same host must use non-overlapping UDP port ranges. For example, keep the first unit on `3478-3497`, copy it to `/etc/systemd/system/apprtc-sfu-2.service`, and give the second process `--media-port-min 3498 --media-port-max 3517` plus a separate log path. Usually omit `--instance-id`: each process generates a distinct process-incarnation ID and retains it across transient reconnects within that process. If the worker process restarts, the new generated ID intentionally prevents an empty engine from claiming the old process's live rooms.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-sfu-2
sudo systemctl status apprtc-sfu apprtc-sfu-2
```

Open the second UDP range in both host and provider firewalls. Do not run two workers on the same UDP ports/address.

## Verify production

```bash
curl -fsS https://appr.tc/status
curl -fsS https://appr.tc/params
```

The `/params` response should advertise `wss://appr.tc:8443/ws` as `wss_url`.

AppWeb receives the public browser WebSocket URL through `--ws-url`. Its private room-authority traffic independently
uses `--grpc-url https://appr.tc:50051`; no `/app` WebSocket endpoint is exposed.

## Certificate renewal

Restart the corresponding service after renewal:

```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh >/dev/null <<'EOF'
#!/bin/sh
systemctl restart apprtc-signaling apprtc-appweb apprtc-sfu
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh
sudo certbot renew --dry-run
```

## CLI reference

Run `appweb --help`, `signaling --help`, and `sfu --help` for the authoritative options. AppWeb requires `--public-url` and `--ws-url`; it also supports `--host-ip`, `--port`, `--grpc-url`, `--insecure-tls`, `--web-root`, HTTP TLS certificate options, ICE options, banner configuration, and `--bypass-join-confirmation`. Signaling supports `--host-ip`, `--port`, `--grpc-port`, and one `--tls`/certificate configuration shared by both listeners; it has no `--public-url` option. SFU supports `--host-ip`, `--media-public-ip`, `--media-port-min`, `--media-port-max`, `--grpc-url`, `--insecure-tls`, advertised capacities, and an optional process-incarnation `--instance-id`; its `--port`, `--redirect-url`, and TLS certificate options apply only to its optional HTTP redirect endpoint. All three binaries support `--debug` (`-d`), `--level` (`-l`), and `--output-log-file` (`-o`). AppWeb's `--ws-url` is the authoritative value returned to browsers. Keep port `50051` inaccessible from untrusted networks until mTLS client authentication is implemented.
