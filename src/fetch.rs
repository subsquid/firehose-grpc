use crate::firehose::Firehose;
use crate::pbfirehose::{fetch_server::Fetch, SingleBlockRequest, SingleBlockResponse};
use std::sync::Arc;
use tracing::error;

pub struct PortalFetch {
    firehose: Arc<Firehose>,
}

impl PortalFetch {
    pub fn new(firehose: Arc<Firehose>) -> PortalFetch {
        PortalFetch { firehose }
    }
}

#[tonic::async_trait]
impl Fetch for PortalFetch {
    async fn block(
        &self,
        request: tonic::Request<SingleBlockRequest>,
    ) -> Result<tonic::Response<SingleBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let response = match self.firehose.block(request).await {
            Ok(response) => response,
            Err(e) => {
                error!("failed to fetch block: {}", e);
                return Err(tonic::Status::unavailable("operation failed"));
            }
        };

        Ok(tonic::Response::new(response))
    }
}
