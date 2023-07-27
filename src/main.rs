use tonic::{transport::Server, Request, Response, Status};
use firehose::stream_server::{Stream, StreamServer};
use codec::Block;
use prost::Message;
use archive::Archive;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use std::time::Duration;

mod archive;

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

async fn resolve_negative_start_block_num(start_block_num: i64, archive: &archive::Archive) -> u64 {
    if start_block_num < 0 {
        let delta = u64::try_from(start_block_num.abs()).unwrap();
        let head = archive.height().await.unwrap();
        return head.saturating_sub(delta);
    }
    u64::try_from(start_block_num).unwrap()
}

fn vec_from_hex(value: &str) -> Result<Vec<u8>, prefix_hex::Error> {
    let buf: Vec<u8> = if value.len() % 2 != 0 {
        let value = format!("0x0{}", &value[2..]);
        prefix_hex::decode(value)?
    } else {
        prefix_hex::decode(value)?
    };

    Ok(buf)
}

#[derive(Debug)]
pub struct ArchiveStream {
    archive: Arc<Archive>,
}

#[tonic::async_trait]
impl Stream for ArchiveStream {
    type BlocksStream = ReceiverStream<Result<firehose::Response, Status>>;

    async fn blocks(&self, request: Request<firehose::Request>) -> Result<Response<Self::BlocksStream>, Status> {
        let mut filters = vec![];
        for transform in &request.get_ref().transforms {
            let filter = transforms::CombinedFilter::decode(&transform.value[..]).unwrap();
            filters.push(filter);
        }

        let mut from_block = resolve_negative_start_block_num(request.get_ref().start_block_num, &self.archive).await;
        let to_block = if request.get_ref().stop_block_num == 0 {
            None
        } else {
            Some(request.get_ref().stop_block_num)
        };

        dbg!(request.get_ref());
        dbg!(&filters);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let archive = self.archive.clone();
        tokio::spawn(async move {
            loop {
                let height = archive.height().await.unwrap();
                let blocks = archive.query(from_block, to_block).await.unwrap();
                let last_block_num = blocks[blocks.len() - 1].header.number;
                for block in blocks {
                    let graph_block = Block {
                        ver: 2,
                        hash: prefix_hex::decode(block.header.hash.clone()).unwrap(),
                        number: block.header.number,
                        size: block.header.size,
                        header: Some(codec::BlockHeader {
                            parent_hash: prefix_hex::decode(block.header.parent_hash).unwrap(),
                            uncle_hash: prefix_hex::decode(block.header.sha3_uncles).unwrap(),
                            coinbase: prefix_hex::decode(block.header.miner).unwrap(),
                            state_root: prefix_hex::decode(block.header.state_root).unwrap(),
                            transactions_root: prefix_hex::decode(block.header.transactions_root).unwrap(),
                            receipt_root: prefix_hex::decode(block.header.receipts_root).unwrap(),
                            logs_bloom: prefix_hex::decode(block.header.logs_bloom).unwrap(),
                            difficulty: Some(codec::BigInt { bytes: vec_from_hex(&block.header.difficulty).unwrap() }),
                            total_difficulty: Some(codec::BigInt { bytes: vec_from_hex(&block.header.total_difficulty).unwrap() }),
                            number: block.header.number,
                            gas_limit: u64::from_str_radix(&block.header.gas_limit.trim_start_matches("0x"), 16).unwrap(),
                            gas_used: u64::from_str_radix(&block.header.gas_used.trim_start_matches("0x"), 16).unwrap(),
                            timestamp: Some(prost_types::Timestamp {
                                seconds: i64::try_from(block.header.timestamp).unwrap(),
                                nanos: 0,
                            }),
                            extra_data: prefix_hex::decode(block.header.extra_data).unwrap(),
                            mix_hash: prefix_hex::decode(block.header.mix_hash).unwrap(),
                            nonce: u64::from_str_radix(&block.header.nonce.trim_start_matches("0x"), 16).unwrap(),
                            hash: prefix_hex::decode(block.header.hash).unwrap(),
                            base_fee_per_gas: block.header.base_fee_per_gas
                                .and_then(|val| Some(codec::BigInt { bytes: vec_from_hex(&val).unwrap() })),
                        }),
                        uncles: vec![],
                        transaction_traces: vec![],
                        balance_changes: vec![],
                        code_changes: vec![],
                    };
                    tx.send(Ok(firehose::Response {
                        block: Some(prost_types::Any {
                            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                            value: graph_block.encode_to_vec(),
                        }),
                        step: 1,
                        cursor: request.get_ref().cursor.clone(),
                    })).await.unwrap();
                }

                if let Some(to_block) = to_block {
                    if u64::from(last_block_num) == to_block {
                        break;
                    }
                }

                if height <= last_block_num {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }

                from_block = last_block_num.into();
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:13042".parse()?;
    let stream = ArchiveStream { archive: Arc::new(Archive::new()) };
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(firehose::FILE_DESCRIPTOR_SET)
        .build()?;

    Server::builder()
        .add_service(StreamServer::new(stream))
        .add_service(reflection)
        .serve(addr)
        .await?;

    Ok(())
}
