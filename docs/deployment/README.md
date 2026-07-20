# AppRTC Deployment

This guide deploys the Rust AppRTC P2P V1 implementation as two services on one host. AppWeb serves the browser application and HTTP room APIs. Signaling owns room state, serves the public browser WebSocket endpoint, and exposes a private gRPC listener to AppWeb on loopback.

```text
Browser ── HTTPS ──> AppWeb (https://appr.tc:443)
Browser ── WSS ────> Signaling (wss://appr.tc:8443/ws)
AppWeb  ── gRPC/HTTP2/TLS ──> Signaling (https://appr.tc:50051)
```

V2/SFU call-mode transitions are not enabled yet.

## DNS and firewall

Point `appr.tc` at the host. Allow TCP `443` for AppWeb and TCP `8443` for signaling. Port `80` is only needed for
Certbot standalone validation.

* **A Record** pointing `@` to the AppRTC server IP (e.g., `173.249.199.192`)
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
cargo build --release -p apprtc --bin appweb --bin signaling
chmod +x /opt/apprtc/target/release/appweb
chmod +x /opt/apprtc/target/release/signaling
```

For an upgrade after the systemd units below have already been installed, restart both services with:

```bash
sudo systemctl restart apprtc-signaling apprtc-appweb
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
ExecStart=/opt/apprtc/target/release/signaling --host-ip 0.0.0.0 --public-url wss://appr.tc:8443 --port 8443 --grpc-port 50051 --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/signaling.log
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
ExecStart=/opt/apprtc/target/release/appweb --host-ip 0.0.0.0 --public-url https://appr.tc --signaling-ws-url wss://appr.tc:8443/ws --signaling-grpc-url https://appr.tc:50051 --port 443 --web-root /opt/apprtc/appweb --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/logs/appweb.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

The shared `--tls` flag protects both signaling listeners with the `appr.tc` certificate. The gRPC listener binds `0.0.0.0` so AppWeb can connect with the certificate-valid hostname `https://appr.tc:50051`; keep TCP `50051` blocked by the host/provider firewalls so it remains reachable only locally. Enable both services:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc-signaling
sudo systemctl enable --now apprtc-appweb
sudo systemctl status apprtc-signaling apprtc-appweb
```

The services handle SIGINT gracefully by draining HTTP/gRPC requests, closing WebSocket connections, and releasing signaling state.

## Local two-process test

The bundled certificate is self-signed. Start signaling and AppWeb on separate local ports in separate terminals (or
background the first command):

```bash
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 --grpc-port 50051 --tls
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb --public-url https://127.0.0.1:8080 --signaling-ws-url wss://127.0.0.1:8081/ws --signaling-grpc-url https://127.0.0.1:50051 --signaling-insecure-tls --tls
```

Then run:

```bash
cargo test -p apprtc --test '*' -- --nocapture
```

Browser clients need to trust the bundled certificate used by the local HTTPS and WSS listeners. The same `--tls` flag also protects the gRPC listener, so AppWeb uses `https://127.0.0.1:50051`; `--signaling-insecure-tls` is required only for this bundled self-signed development certificate.

## Verify production

```bash
curl -fsS https://appr.tc/status
curl -fsS https://appr.tc/params
```

The `/params` response should advertise `wss://appr.tc:8443/ws` as `wss_url`.

AppWeb receives the public browser WebSocket URL through `--signaling-ws-url`. Its private room-authority traffic independently uses `--signaling-grpc-url https://appr.tc:50051`; no `/app` WebSocket endpoint is exposed.

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

Run `appweb --help` and `signaling --help` for the authoritative options. Both support `--host-ip`, `--port`, `--tls`, `--certificate`, `--private-key`, `--debug` (`-d`), `--level` (`-l`), and `--output-log-file` (`-o`). AppWeb requires `--public-url` and `--signaling-ws-url`; it also supports `--signaling-grpc-url`, `--signaling-token`, `--signaling-insecure-tls`, `--web-root`, ICE options, banner configuration, and `--bypass-join-confirmation`. Signaling accepts `--public-url` and supports `--grpc-port` and `--grpc-token` for its private service API. Signaling's `--host-ip` applies to both listeners, and its `--tls` flag protects both listeners with the same certificate. In the current P2P V1 implementation, AppWeb's `--signaling-ws-url` is the authoritative value returned to browsers; signaling's `--public-url` does not replace it.
