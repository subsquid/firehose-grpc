fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .out_dir("src/protobuf")
        .file_descriptor_set_path(out_dir.join("firehose_descriptor.bin"))
        .compile_protos(&["proto/firehose.proto"], &["proto"])?;
    tonic_build::configure()
        .out_dir("src/protobuf")
        .compile_protos(&["proto/transforms.proto"], &["proto"])?;
    tonic_build::configure()
        .out_dir("src/protobuf")
        .compile_protos(&["proto/codec.proto"], &["proto"])?;
    Ok(())
}
