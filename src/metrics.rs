use prometheus::{opts, register_int_counter, register_int_gauge, IntCounter, IntGauge, TextEncoder, gather, Encoder};
use axum::response::Response;
use axum::http::header::CONTENT_TYPE;
use axum::body::Body;
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;

lazy_static::lazy_static! {
    pub static ref ACTIVE_REQUESTS: IntGauge = register_int_gauge!(
        opts!("firehose_active_requests", "Number of active requests")
    ).expect("Can't create a metric");
    pub static ref REQUESTS_COUNTER: IntCounter = register_int_counter!(
        opts!("firehose_requests_counter", "Request count")
    ).expect("Can't create a metric");
}

pub async fn start_prometheus_server() -> anyhow::Result<()> {
    let router = Router::new().route("/metrics", get(get_metrics));
    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    tokio::spawn(async {
        axum::serve(listener, router).await.unwrap();
    });
    Ok(())
}

async fn get_metrics() -> Response {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder
        .encode(&gather(), &mut buffer)
        .expect("Failed to encode metrics");
    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap()
}
