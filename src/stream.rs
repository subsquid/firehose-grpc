use crate::firehose::Firehose;
use crate::pbfirehose::{stream_server::Stream, Request, Response};
use crate::metrics;
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error};

pub struct PortalStream {
    firehose: Arc<Firehose>,
}

impl PortalStream {
    pub fn new(firehose: Arc<Firehose>) -> PortalStream {
        PortalStream { firehose }
    }
}

#[tonic::async_trait]
impl Stream for PortalStream {
    type BlocksStream = ReceiverStream<Result<Response, tonic::Status>>;

    async fn blocks(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status> {
        metrics::REQUESTS_COUNTER.inc();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let request = request.into_inner();
        let firehose = self.firehose.clone();

        tokio::spawn(async move {
            let stream = match firehose.blocks(&request).await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("failed to establish block stream: {}", e);
                    return;
                }
            };

            debug!("block stream established successfully");
            metrics::ACTIVE_REQUESTS.inc();

            tokio::pin!(stream);

            while let Some(result) = stream.next().await {
                match result {
                    Ok(response) => {
                        if let Err(e) = tx.send(Ok(response)).await {
                            debug!("block stream has been closed: {}", e);
                            metrics::ACTIVE_REQUESTS.dec();
                            return;
                        }
                    }
                    Err(e) => {
                        error!("error while streaming data: {}", e);
                        metrics::ACTIVE_REQUESTS.dec();
                        return;
                    }
                }
            }

            debug!("block stream finished");
            metrics::ACTIVE_REQUESTS.dec();
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
