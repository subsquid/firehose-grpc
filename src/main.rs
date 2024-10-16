use portal::Portal;
use clap::Parser;
use cli::Cli;
use datasource::HotDataSource;
use ds_portal::PortalDataSource;
use ds_rpc::RpcDataSource;
use fetch::PortalFetch;
use firehose::Firehose;
use pbfirehose::{fetch_server::FetchServer, stream_server::StreamServer};
use std::sync::Arc;
use stream::PortalStream;
use tonic::transport::Server;
use tracing::info;

mod portal;
mod cli;
mod cursor;
mod datasource;
mod ds_portal;
mod ds_rpc;
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

    let args = Cli::parse();

    let rpc_ds: Option<Arc<dyn HotDataSource + Sync + Send>> = if let Some(rpc) = args.rpc {
        let finality_confirmation = args
            .finality_confirmation
            .expect("finality_confirmation is required if rpc is specified");
        Some(Arc::new(RpcDataSource::new(rpc, finality_confirmation)))
    } else {
        None
    };

    let portal = Arc::new(Portal::new(args.portal));
    let portal_ds = Arc::new(PortalDataSource::new(portal));
    let firehose = Arc::new(Firehose::new(portal_ds, rpc_ds));

    let stream_service = StreamServer::new(PortalStream::new(firehose.clone()));
    let fetch_service = FetchServer::new(PortalFetch::new(firehose));
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
