use crate::firehose::Firehose;
use crate::pbfirehose::{fetch_server::Fetch, SingleBlockRequest, SingleBlockResponse};
use std::sync::Arc;

pub struct ArchiveFetch {
    firehose: Arc<Firehose>,
}

impl ArchiveFetch {
    pub fn new(firehose: Arc<Firehose>) -> ArchiveFetch {
        ArchiveFetch { firehose }
    }
}

#[tonic::async_trait]
impl Fetch for ArchiveFetch {
    async fn block(
        &self,
        request: tonic::Request<SingleBlockRequest>,
    ) -> Result<tonic::Response<SingleBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let response = self.firehose.block(request).await.unwrap();

        Ok(tonic::Response::new(response))
    }
}
