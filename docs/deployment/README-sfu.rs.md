# AppRTC Deployment - sfu.rs

This guide deploys Rust AppRTC's dedicated SFU worker run on `sfu.rs`. The SFU owns the UDP media ports and maintains
one bidirectional gRPC session to signaling.

```text
Browser ── HTTPS ──> AppWeb (https://appr.tc:443)
Browser ── WSS ────> Signaling (wss://appr.tc:8443/ws)
AppWeb  ── gRPC/HTTP2/TLS ──> Signaling (https://appr.tc:50051)
SFU 2   ── gRPC/HTTP2/TLS ──> Signaling (https://appr.tc:50051)
Browser <── ICE/DTLS/SRTP over UDP ──> SFU (sfu.rs:3478-3497)
```

V1 remains backward compatible. In V2, the first two participants use P2P; a third participant triggers P2P→SFU upgrade.
SFU→P2P downgrade is not implemented yet.

## DNS and firewall

Point `sfu.rs` at the host. Allow TCP `443` for the optional redirect and UDP `3478-3497` for media on the SFU host. Port `80` is only needed for Certbot standalone validation. The SFU host also needs outbound TCP access to signaling at `appr.tc:50051`; the signaling host/provider firewall must allow inbound TCP `50051` only from this SFU host's public IP (and any other trusted AppWeb/SFU hosts).

* **A Record** pointing `@` to the server IP (e.g., `173.249.204.140`)
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

Obtain a certificate for `sfu.rs`:

```bash
sudo certbot certonly --standalone -d sfu.rs -d www.sfu.rs --agree-tos
```

The binaries accept the certificate and key directly:

```text
/etc/letsencrypt/live/sfu.rs/fullchain.pem
/etc/letsencrypt/live/sfu.rs/privkey.pem
```

## Install and build

Copy the repository to the host:

```bash
rsync -avz --exclude target --exclude .git --exclude .idea ./ root@173.249.204.140:/opt/apprtc/
```

Build the required binaries:

```bash
cd /opt/apprtc
cargo build --release --bin sfu
chmod +x /opt/apprtc/target/release/sfu
```

For an upgrade after the systemd unit below has already been installed, restart the service with:

```bash
sudo systemctl restart apprtc-sfu
```

## Production services

Run the SFU on the same host. Replace the example public IP with the address resolved by `sfu.rs`:

```bash
nano /etc/systemd/system/apprtc-sfu.service
```

```ini
[Unit]
Description=AppRTC SFU media worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStartPre=/bin/sh -c 'mkdir -p /opt/logs; if [ -f /opt/logs/sfu.log ]; then mv /opt/logs/sfu.log /opt/logs/sfu-$(date +%%Y%%m%%d-%%H%%M%%S).log; fi'
ExecStart=/opt/apprtc/target/release/sfu --host-ip 0.0.0.0 --port 443 --redirect-url https://appr.tc/ --media-public-ip 173.249.204.140 --media-port-min 3478 --media-port-max 3497 --grpc-url https://appr.tc:50051 --tls --certificate /etc/letsencrypt/live/sfu.rs/fullchain.pem --private-key /etc/letsencrypt/live/sfu.rs/privkey.pem -d -l info -o /opt/logs/sfu.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

`--tls`, `--certificate`, and `--private-key` protect only the optional redirect endpoint; the `https://` scheme in `--grpc-url` independently enables server-authenticated TLS for the outbound gRPC session. Do not use `--insecure-tls` in production. Enable the SFU service on the host:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-sfu
sudo systemctl status apprtc-sfu
```

The service handles SIGINT gracefully by closing the SFU session and peer connections and releasing media state.

## Worker registration and load balancing

The process opens one reconnecting `OpenSfuSession` stream to signaling and reports `Ready` health plus its `--max-rooms` and `--max-clients` capacity. Signaling selects among eligible workers by the lowest tuple `(assigned_clients, assigned_rooms, instance_id)` and pins each upgraded room to one worker.

Normally omit `--instance-id`. The process generates a unique incarnation ID once at startup and reuses it if its gRPC stream reconnects. A process restart generates a new ID by design: an empty replacement process must not claim WebRTC state owned by the previous process. If the old process does not reconnect during signaling's grace period, its established rooms fail with `room-failed`; they are not migrated automatically to another worker.

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
systemctl restart apprtc-sfu
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh
sudo certbot renew --dry-run
```

## CLI reference

Run `sfu --help` for the authoritative options. The SFU supports `--host-ip`, `--media-public-ip`, `--media-port-min`, `--media-port-max`, `--grpc-url`, `--insecure-tls`, `--max-rooms`, `--max-clients`, and an optional process-incarnation `--instance-id`. Its `--port`, `--redirect-url`, `--tls`, `--certificate`, and `--private-key` options configure only the optional landing-page redirect server. `--debug` (`-d`), `--level` (`-l`), and `--output-log-file` (`-o`) configure logging. On the signaling host, keep port `50051` inaccessible from all sources except the trusted AppWeb and SFU hosts until mTLS client authentication is implemented.
