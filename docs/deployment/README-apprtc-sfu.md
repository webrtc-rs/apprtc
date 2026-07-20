# AppRTC Deployment

This guide deploys the Rust AppRTC P2P V1 implementation as two services on separate machines. AppWeb serves the browser application and HTTP room APIs. Signaling owns room state, exposes the public browser WebSocket endpoint, and exposes a private TLS-protected gRPC listener to AppWeb.

```text
Browser ── HTTPS ──> AppWeb (https://appr.tc)
Browser ── WSS ────> Signaling (wss://sfu.rs/ws)
AppWeb  ── gRPC/HTTP2/TLS ──> Signaling (https://sfu.rs:50051)
```

V2/SFU call-mode transitions are not enabled yet.

## DNS and firewall

Point `appr.tc` at the AppWeb host and `sfu.rs` at the signaling host. Allow TCP `443` on both hosts. Allow signaling TCP `50051` only from the AppWeb host's source address; do not expose the private gRPC listener to the general Internet. Port `80` is only needed for Certbot standalone validation.

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
```

Build the required binaries:

```bash
cd /opt/apprtc
cargo build --release -p apprtc --bin appweb --bin signaling
chmod +x /opt/apprtc/target/release/appweb
chmod +x /opt/apprtc/target/release/signaling
```

## Production services

Run signaling on the signaling host:

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

```ini
[Unit]
Description=AppRTC AppWeb HTTP server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStartPre=/bin/sh -c 'mkdir -p /opt/logs; if [ -f /opt/logs/appweb.log ]; then mv /opt/logs/appweb.log /opt/logs/appweb-$(date +%%Y%%m%%d-%%H%%M%%S).log; fi'
ExecStart=/opt/apprtc/target/release/appweb --host-ip 0.0.0.0 --public-url https://appr.tc --signaling-url wss://sfu.rs/ws --signaling-grpc-url https://sfu.rs:50051 --port 443 --web-root /opt/apprtc/appweb --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/appweb.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Do not use `--signaling-insecure-tls` in production. Enable both services:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-signaling
sudo systemctl enable --now apprtc-appweb
sudo systemctl status apprtc-signaling apprtc-appweb
```

The services handle SIGINT gracefully by draining HTTP/gRPC requests, closing WebSocket connections, and releasing signaling state.

## Local two-process test

The bundled certificate is self-signed. Start signaling and AppWeb on separate local ports:

```bash
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 --grpc-port 50051 --tls
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb --public-url https://127.0.0.1:8080 --signaling-url wss://127.0.0.1:8081/ws --signaling-grpc-url https://127.0.0.1:50051 --signaling-insecure-tls --tls
```

Then run:

```bash
cargo test -p apprtc --test '*' -- --nocapture
```

Browser clients still need to trust the bundled certificate. The signaling `--tls` flag protects both WSS and gRPC, and AppWeb's `--signaling-insecure-tls` accepts that self-signed certificate only for local development.

## Verify production

```bash
curl -fsS https://appr.tc/status
curl -fsS https://appr.tc/params
```

The `/params` response should advertise `wss://sfu.rs/ws` as `wss_url`. AppWeb uses the independent `--signaling-grpc-url https://sfu.rs:50051` setting for its private unary gRPC calls.

## Certificate renewal

Restart the corresponding service after renewal:

```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh >/dev/null <<'EOF'
#!/bin/sh
systemctl restart apprtc-signaling apprtc-appweb
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh
sudo certbot renew --dry-run
```

## CLI reference

Run `appweb --help` and `signaling --help` for the authoritative options. AppWeb uses `--signaling-url` for the public browser WebSocket and `--signaling-grpc-url` for the private service channel. Signaling uses `--host-ip` for both listener bind addresses and `--grpc-port` for the private listener's port, while its shared `--tls` setting protects both listeners. Both sides support an optional shared bearer credential with AppWeb `--signaling-token` and signaling `--grpc-token`; mTLS should replace a shared token when service identities are available.
