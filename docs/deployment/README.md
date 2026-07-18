# AppRTC Deployment

This guide deploys the Rust AppRTC binary as an all-in-one P2P V1 signaling server. The binary serves the AppRTC web
application, HTTP room APIs, WebSocket signaling, and optional HTTPS/WSS from one process.

The current binary does not enable V2/SFU call-mode transitions. The historical Go deployment is retained on the
repository's `go` branch.

## 1. DNS and firewall

Point the domain's `A`/`AAAA` records at the server and allow TCP ports `80` and `443` as needed. Port `80` is only
required when using Certbot's standalone HTTP validation.

* **A Record** pointing `@` to server IP (e.g., `173.249.199.192`)
* **A Record** pointing `www` to server IP (e.g., `173.249.199.192`)
* **Note**: Make sure to delete any conflicting CNAME or AAAA (IPv6) records.

## 2. Install prerequisites

On Fedora:

```bash
sudo dnf install -y git rsync certbot rust cargo
```

If the distribution Rust toolchain is too old for Edition 2024, install the current stable toolchain with `rustup`
instead. Or

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 3. Obtain TLS certificates

Stop any service currently using the validation port, then request a certificate:

```bash
sudo systemctl stop apprtc || true
sudo certbot certonly --standalone -d appr.tc -d www.appr.tc --agree-tos
```

The Rust binary accepts the certificate and key directly; no `/cert` symlinks are required:

```text
/etc/letsencrypt/live/appr.tc/fullchain.pem
/etc/letsencrypt/live/appr.tc/privkey.pem
```

## 4. Install the source tree

Copy the repository to a location executable by systemd and readable by the service:

```bash
rsync -avz --exclude 'target' --exclude '.git' --exclude '.idea' ./ root@173.249.199.192:/opt/apprtc/
```

The web assets must remain at `/opt/apprtc/appweb` unless `--web-root` is changed.

## 5. Build the release binary

On the server:

```bash
sudo mkdir -p /opt/log
cd /opt/apprtc
cargo build --release -p apprtc --bin apprtc
chmod +x /opt/apprtc/target/release/apprtc
```

or you can install to /usr/local/bin/

```bash
sudo install -m 0755 /opt/apprtc/target/release/apprtc /usr/local/bin/apprtc
```

## 6. Configure systemd

Create a service file:

```bash
nano /etc/systemd/system/apprtc.service
```

Add the following content:

```ini
[Unit]
Description=AppRTC P2P/SFU Signaling Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apprtc
ExecStart=/opt/apprtc/target/release/apprtc --host-ip 0.0.0.0 --public-url https://appr.tc --port 443 --web-root /opt/apprtc/appweb --tls --certificate /etc/letsencrypt/live/appr.tc/fullchain.pem --private-key /etc/letsencrypt/live/appr.tc/privkey.pem -d -l info -o /opt/log/apprtc.log
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now apprtc
sudo systemctl status apprtc
```

Stop it:

```bash
sudo systemctl stop apprtc || true
```

The process handles Ctrl-C/SIGINT gracefully: it closes signaling WebSockets, drains HTTP/TLS connections, and exits
cleanly.

## 7. Verify the deployment

```bash
curl -fsS https://appr.tc/status
curl -fsS https://appr.tc/params
```

Open `https://appr.tc/` in a browser. `/status` reports uptime, open WebSockets, total WebSockets, WebSocket errors, and
HTTP errors.

## 8. Certificate renewal

Certbot normally installs a renewal timer. Because AppRTC loads certificates at startup, restart it after a successful
renewal:

```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh >/dev/null <<'EOF'
#!/bin/sh
systemctl restart apprtc
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-apprtc.sh
sudo certbot renew --dry-run
```

Check the timer with:

```bash
systemctl list-timers --all | grep certbot
```

## 9. Useful CLI options

```text
--host-ip HOST-IP              Local listener bind address (default: 127.0.0.1)
--public-url URL               Browser-facing HTTP(S) origin for generated URLs
--port PORT                    HTTP/HTTPS port (default: 8080)
--web-root PATH                AppRTC assets (default: appweb)
--tls                          Enable HTTPS/WSS
--certificate PATH             PEM certificate chain
--private-key PATH             PEM private key
--ice-server-url URL           Repeat or provide comma-separated ICE URLs
--ice-server-base-url URL      External ICE credential service origin
--ice-server-api-key KEY       Credential-service API key
--header-message TEXT          Page banner
--bypass-join-confirmation     Skip the browser confirmation prompt
--level LEVEL                  error, warn, info, debug, or trace
--output-log-file PATH         Write formatted logs to a file
```

Run `/opt/apprtc/target/release/apprtc -h` for the authoritative option list.
