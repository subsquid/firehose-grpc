fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("firehose_descriptor.bin"))
        .compile(&["proto/firehose.proto"], &["proto"])?;
    tonic_build::compile_protos("proto/transforms.proto")?;
    tonic_build::compile_protos("proto/codec.proto")?;
    Ok(())
}
