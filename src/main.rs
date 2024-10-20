use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use firehose_grpc::ds_portal::PortalDataSource;
use firehose_grpc::cli::Cli;
use firehose_grpc::ds_rpc::RpcDataSource;
use firehose_grpc::fetch::PortalFetch;
use firehose_grpc::firehose::Firehose;
use firehose_grpc::pbfirehose::{fetch_server::FetchServer, stream_server::StreamServer};
use firehose_grpc::stream::PortalStream;
use firehose_grpc::metrics::start_prometheus_server;
use firehose_grpc::portal::Portal;
use firehose_grpc::datasource::HotDataSource;
use firehose_grpc::logger;

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

    start_prometheus_server().await?;
    info!("prometheus metrics are available at 0.0.0.0:3000");

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
