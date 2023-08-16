use crate::firehose::Firehose;
use crate::pbfirehose::{stream_server::Stream, Request, Response};
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct ArchiveStream {
    firehose: Arc<Firehose>,
}

impl ArchiveStream {
    pub fn new(firehose: Arc<Firehose>) -> ArchiveStream {
        ArchiveStream { firehose }
    }
}

#[tonic::async_trait]
impl Stream for ArchiveStream {
    type BlocksStream = ReceiverStream<Result<Response, tonic::Status>>;

    async fn blocks(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let request = request.into_inner();
        let firehose = self.firehose.clone();

        tokio::spawn(async move {
            let stream = firehose.blocks(request).await.unwrap();

            tokio::pin!(stream);

            while let Some(response) = stream.next().await {
                if let Err(_) = tx.send(Ok(response)).await {
                    break;
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
