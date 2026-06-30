# AppRTC Deployment

This plan details how to deploy the unified **AppRTC P2P signaling server** natively to a remote Fedora 42 server using Let's Encrypt SSL certificates.

---

## 1. Domain & DNS Configuration
Configure your domain registrar's DNS Management panel to point your domain (e.g. `appr.tc`) to your remote server:

* **A Record** pointing `@` to your server IP (e.g., `173.249.199.192`)
* **A Record** pointing `www` to your server IP (e.g., `173.249.199.192`)
* **Note**: Make sure to delete any conflicting CNAME or AAAA (IPv6) records.

---

## 2. Remote Server Prerequisites
On the remote Fedora 42 SSH terminal, install Go, Git, Rsync, and Certbot:
```bash
dnf install -y golang git rsync certbot
```

---

## 3. Obtain Let's Encrypt SSL Certificates
Obtain a free, trusted SSL certificate. Make sure port `80` is free (stop any running web servers or the apprtc service first):
```bash
systemctl stop apprtc || true
certbot certonly --standalone -d appr.tc -d www.appr.tc --register-unsafely-without-email --agree-tos
```

Create a `/cert` directory and link the generated Let's Encrypt certificates to the expected paths:
```bash
mkdir -p /cert
ln -sf /etc/letsencrypt/live/appr.tc/fullchain.pem /cert/cert.pem
ln -sf /etc/letsencrypt/live/appr.tc/privkey.pem /cert/key.pem
```

---

## 4. Copy Project Files to Remote
From your **local terminal** (run this inside the project root on your MacBook), upload the code to `/opt/apprtc` using `rsync`:
```bash
rsync -avz --exclude 'target' --exclude '.git' --exclude '.idea' ./ root@173.249.199.192:/opt/apprtc/
```
*Note: We use `/opt/apprtc` because Fedora's default SELinux security policy prevents systemd from executing binaries inside `/root`.*

---

## 5. Build the Server on Remote
On the **remote server**, compile the server binary and grant it execution permissions:
```bash
cd /opt/apprtc/collider
go build -o /opt/apprtc/collider_bin collidermain/main.go
chmod +x /opt/apprtc/collider_bin
```

---

## 6. Run as a Systemd Background Service
To run the server continuously in the background on port `443` (HTTPS/WSS):

1. Create a service file:
   ```bash
   nano /etc/systemd/system/apprtc.service
   ```

2. Add the following content:
   ```ini
   [Unit]
   Description=AppRTC Room and Signaling Server
   After=network.target

   [Service]
   Type=simple
   WorkingDirectory=/opt/apprtc
   ExecStart=/opt/apprtc/collider_bin -port=443 -tls=true -web-root=/opt/apprtc/web_app
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

3. Enable and start the service:
   ```bash
   systemctl daemon-reload
   systemctl enable --now apprtc
   ```

4. Verify the status:
   ```bash
   systemctl status apprtc
   ```