# 1. Start Signaling.
cargo run -p apprtc --bin signaling -- --host-ip 127.0.0.1 --port 8081 \
  --grpc-port 50051 --tls -d -l debug -o ~/Downloads/signaling.log &

# 2. Start one SFU worker.
cargo run -p apprtc --bin sfu -- --host-ip 127.0.0.1 --public-ip 127.0.0.1 \
  --media-port-min 35000 --media-port-max 35000 \
  --grpc-url https://127.0.0.1:50051 --insecure-tls -d -l debug -o ~/Downloads/sfu.log &

# 3. Start AppWeb.
cargo run -p apprtc --bin appweb -- --host-ip 127.0.0.1 --port 8080 --web-root appweb \
  --public-url https://127.0.0.1:8080 --ws-url wss://127.0.0.1:8081/ws \
  --grpc-url https://127.0.0.1:50051 --insecure-tls --tls -d -l debug -o ~/Downloads/appweb.log &

# 4. Run the integration tests.
# cargo test -p apprtc --test '*' -- --nocapture
