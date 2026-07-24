#!/bin/bash

mkdir -p /tmp/logs

if [ -f /tmp/logs/signaling.log ]; then mv /tmp/logs/signaling.log /tmp/logs/signaling-$(date +%Y%m%d-%H%M%S).log; fi
if [ -f /tmp/logs/appweb.log ]; then mv /tmp/logs/appweb.log /tmp/logs/appweb-$(date +%Y%m%d-%H%M%S).log; fi
if [ -f /tmp/logs/sfu.log ]; then mv /tmp/logs/sfu.log /tmp/logs/sfu-$(date +%Y%m%d-%H%M%S).log; fi

# 1. Start Signaling.
cargo run --bin signaling -- --host-ip 127.0.0.1 --port 8081 \
  --grpc-port 50051 --tls -d -l info -o /tmp/logs/signaling.log &

# 2. Start one SFU worker.
cargo run --bin sfu -- --host-ip 127.0.0.1 --port 8082 \
  --media-port-min 35000 --media-port-max 35000 --redirect-url https://127.0.0.1:8080 \
  --grpc-url https://127.0.0.1:50051 --insecure-tls --tls -d -l info -o /tmp/logs/sfu.log &

# 3. Start AppWeb.
cargo run --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url https://127.0.0.1:8080 --ws-url wss://127.0.0.1:8081/ws \
  --grpc-url https://127.0.0.1:50051 --insecure-tls --tls -d -l info -o /tmp/logs/appweb.log &

# 4. Run the integration tests.
# cargo test --test '*' -- --nocapture
