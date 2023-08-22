use archive::Archive;
use fetch::ArchiveFetch;
use firehose::Firehose;
use pbfirehose::{fetch_server::FetchServer, stream_server::StreamServer};
use std::sync::Arc;
use stream::ArchiveStream;
use tonic::transport::Server;
use tracing::info;

mod archive;
mod fetch;
mod firehose;
mod logger;
mod stream;

#[path = "protobuf/sf.firehose.v2.rs"]
mod pbfirehose;

#[path = "protobuf/sf.ethereum.transform.v1.rs"]
mod pbtransforms;

#[path = "protobuf/sf.ethereum.r#type.v2.rs"]
mod pbcodec;

const FIREHOSE_DESCRIPTOR: &[u8] = tonic::include_file_descriptor_set!("firehose_descriptor");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logger::init();

    let archive = Arc::new(Archive::new());
    let firehose = Arc::new(Firehose::new(archive));

    let stream_service = StreamServer::new(ArchiveStream::new(firehose.clone()));
    let fetch_service = FetchServer::new(ArchiveFetch::new(firehose));
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FIREHOSE_DESCRIPTOR)
        .build()?;

    info!("starting firehose-grpc at 0.0.0.0:13042");
    let addr = "0.0.0.0:13042".parse()?;
    Server::builder()
        .add_service(stream_service)
        .add_service(fetch_service)
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
