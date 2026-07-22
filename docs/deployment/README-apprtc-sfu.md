# AppRTC Deployment

This guide deploys Rust AppRTC across two machines: AppWeb runs on `appr.tc`, while signaling and an SFU worker run on `sfu.rs`. AppWeb serves the browser application and HTTP room APIs. Signaling owns V1/V2 room state, exposes the public browser WebSocket endpoint, and exposes a private TLS-protected gRPC listener to AppWeb and SFU. The SFU owns the UDP media ports.

```text
Browser ── HTTPS ──> AppWeb (https://appr.tc)
Browser ── WSS ────> Signaling (wss://sfu.rs/ws)
AppWeb  ── gRPC/HTTP2/TLS ──> Signaling (https://sfu.rs:50051)
SFU     ── gRPC/HTTP2/TLS ──> Signaling (https://sfu.rs:50051)
Browser <── ICE/DTLS/SRTP over UDP ──> SFU (sfu.rs:3478-3495)
```

V1 remains backward compatible. In V2, the first two participants use P2P; a third participant triggers P2P→SFU upgrade. SFU→P2P downgrade is not implemented yet.

## DNS and firewall

Point `appr.tc` at the AppWeb host and `sfu.rs` at the signaling/SFU host. Allow TCP `443` on both hosts and UDP `3478-3495` on the SFU host. Allow signaling TCP `50051` only from the AppWeb host's source address and the signaling/SFU host itself; do not expose the private gRPC listener to the general Internet. Port `80` is only needed for Certbot standalone validation.

* **A Record** pointing `@` to server IP (e.g., `173.249.199.192` for `appr.tc`, `173.249.204.140` for `sfu.rs`)
* **A Record** pointing `www` to server IP (e.g., `173.249.199.192` for `appr.tc`, `173.249.204.140` for `sfu.rs`)
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

Obtain a certificate on each host for its public name:

```bash
sudo certbot certonly --standalone -d appr.tc -d www.appr.tc --agree-tos
```

```bash
sudo certbot certonly --standalone -d sfu.rs -d www.sfu.rs --agree-tos
```

The binaries accept the certificate and key directly:

```text
/etc/letsencrypt/live/appr.tc/fullchain.pem
/etc/letsencrypt/live/appr.tc/privkey.pem
```

```text
/etc/letsencrypt/live/sfu.rs/fullchain.pem
/etc/letsencrypt/live/sfu.rs/privkey.pem
```

## Install and build

Copy the repository to each host, preserving the `appweb` assets on the AppWeb host:

```bash
rsync -avz --exclude target --exclude .git --exclude .idea ./ root@appweb-host:/opt/apprtc/
rsync -avz --exclude target --exclude .git --exclude .idea ./ root@signaling-host:/opt/apprtc/
```

Build `appweb` on the AppWeb host and `signaling` plus `sfu` on the signaling/SFU host. Building all binaries with the following command is also valid on either host:

```bash
cd /opt/apprtc
cargo build --release -p apprtc --bin appweb --bin signaling --bin sfu
chmod +x /opt/apprtc/target/release/appweb
chmod +x /opt/apprtc/target/release/signaling
chmod +x /opt/apprtc/target/release/sfu
```

## Production services

Run signaling on the signaling host:

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
ExecStart=/opt/apprtc/target/release/signaling --host-ip 0.0.0.0 --public-url wss://sfu.rs --port 443 --grpc-port 50051 --tls --certificate /etc/letsencrypt/live/sfu.rs/fullchain.pem --private-key /etc/letsencrypt/live/sfu.rs/privkey.pem -d -l info -o /opt/logs/signaling.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Run AppWeb on the AppWeb host:

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
ExecStart=/opt/apprtc/target/release/appweb --host-ip 0.0.0.0 --public-url https://appr.tc --ws-url wss://sfu.rs/ws --grpc-url https://sfu.rs:50051 --port 443 --web-root /opt/apprtc/appweb --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/appweb.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Run the SFU on the signaling/SFU host. Replace the example public IP with the address resolved by `sfu.rs`:

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
ExecStart=/opt/apprtc/target/release/sfu --host-ip 0.0.0.0 --media-public-ip 173.249.204.140 --media-port-min 3478 --media-port-max 3495 --grpc-url https://sfu.rs:50051 -d -l info -o /opt/logs/sfu.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Do not use `--insecure-tls` in production. Enable signaling on the signaling host:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-signaling
sudo systemctl enable --now apprtc-sfu
sudo systemctl status apprtc-signaling apprtc-sfu
```

Enable AppWeb on the AppWeb host:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-appweb
sudo systemctl status apprtc-appweb
```

The services handle SIGINT gracefully by draining HTTP/gRPC requests, closing WebSocket and SFU peer connections, and releasing signaling/media state.

## Local three-process test

The bundled certificate is self-signed. Start signaling, SFU, and AppWeb on separate local ports:

```bash
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 --grpc-port 50051 --tls
cargo run -p apprtc --bin sfu -- --host-ip 127.0.0.1 --media-port-min 35000 --media-port-max 35000 --grpc-url https://127.0.0.1:50051 --insecure-tls
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb --public-url https://127.0.0.1:8080 --ws-url wss://127.0.0.1:8081/ws --grpc-url https://127.0.0.1:50051 --insecure-tls --tls
```

Then run:

```bash
cargo test -p apprtc --test '*' -- --nocapture
```

Browser clients still need to trust the bundled certificate. The signaling `--tls` flag protects both WSS and gRPC, and AppWeb/SFU `--insecure-tls` accepts that self-signed certificate only for local development.

## Verify production

```bash
curl -fsS https://appr.tc/status
curl -fsS https://appr.tc/params
```

The `/params` response should advertise `wss://sfu.rs/ws` as `wss_url`. AppWeb uses the independent `--grpc-url https://sfu.rs:50051` setting for its private unary gRPC calls.

## Certificate renewal

Restart the corresponding service after renewal. On the signaling host, install a hook that restarts signaling; on the AppWeb host, install an equivalent hook that restarts AppWeb.

```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc-signaling.sh >/dev/null <<'EOF'
#!/bin/sh
systemctl restart apprtc-signaling
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc-signaling.sh
sudo certbot renew --dry-run
```

```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc-appweb.sh >/dev/null <<'EOF'
#!/bin/sh
systemctl restart apprtc-appweb
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc-appweb.sh
sudo certbot renew --dry-run
```

## CLI reference

Run `appweb --help`, `signaling --help`, and `sfu --help` for the authoritative options. AppWeb and signaling support `--host-ip`, `--port`, `--tls`, `--certificate`, and `--private-key`; all three binaries support logging flags. AppWeb requires `--public-url` and `--ws-url` and uses `--grpc-url` for unary authority calls. Signaling uses `--host-ip` for both listener addresses and `--grpc-port` for its private listener; its shared `--tls` setting protects WSS and gRPC. SFU supports `--host-ip`, `--media-public-ip`, its UDP media range, `--grpc-url`, `--insecure-tls`, advertised capacities, and an optional process-incarnation ID. AppWeb's `--ws-url` is the authoritative browser value. Restrict port `50051` to AppWeb and SFU source addresses until mTLS client authentication is implemented.
