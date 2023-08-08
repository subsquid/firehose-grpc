use tonic::transport::Server;
use std::sync::Arc;
use archive::Archive;
use firehose::{stream_server::StreamServer, fetch_server::FetchServer};
use stream::ArchiveStream;
use fetch::ArchiveFetch;

mod archive;
mod stream;
mod fetch;

#[allow(non_snake_case)]
pub mod firehose {
    tonic::include_proto!("sf.firehose.v2");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("firehose_descriptor");
}

#[allow(non_snake_case)]
pub mod transforms {
    tonic::include_proto!("sf.ethereum.transform.v1");
}

#[allow(non_snake_case)]
pub mod codec {
    tonic::include_proto!("sf.ethereum.r#type.v2");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive = Arc::new(Archive::new());
    let stream_service = StreamServer::new(ArchiveStream { archive: archive.clone() });
    let fetch_service = FetchServer::new(ArchiveFetch { archive });
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(firehose::FILE_DESCRIPTOR_SET)
        .build()?;

    let addr = "0.0.0.0:13042".parse()?;
    Server::builder()
        .add_service(stream_service)
        .add_service(fetch_service)
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
