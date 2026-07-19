fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.compile_protos(&["proto/signaling.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/signaling.proto");
    Ok(())
}
