use tonic::{transport::Server, Request, Response, Status};
use firehose::stream_server::{Stream, StreamServer};
use codec::Block;
use prost::Message;

pub mod firehose {
    tonic::include_proto!("sf.firehose.v2");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("firehose_descriptor");
}

pub mod transforms {
    tonic::include_proto!("sf.ethereum.transform.v1");
}

pub mod codec {
    tonic::include_proto!("sf.ethereum.r#type.v2");
}

pub mod archive {
    use serde::{Serialize, Deserialize};
    use serde_json::json;

    struct BatchRequest {}

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct BlockHeader {
        pub number: u32,
        pub hash: String,
        pub parent_hash: String,
        pub size: u32,
        pub sha3_uncles: String,
        pub miner: String,
        pub state_root: String,
        pub transactions_root: String,
        pub receipts_root: String,
        pub logs_bloom: String,
        pub difficulty: String,
        pub total_difficulty: String,
        pub gas_limit: String,
        pub gas_used: String,
        pub timestamp: u64,
        pub extra_data: String,
        pub mix_hash: String,
        pub nonce: String,
        pub base_fee_per_gas: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Block {
        pub header: BlockHeader,
    }

    #[derive(Debug)]
    pub struct Archive {
        client: reqwest::Client,
    }

    impl Archive {
        pub fn new() -> Archive {
            let client = reqwest::Client::new();
            Archive { client }
        }

        pub async fn height(&self) -> Result<u32, reqwest::Error> {
            let height = self.client.get("https://v2.archive.subsquid.io/network/ethereum-mainnet/height")
                .send()
                .await?
                .text()
                .await?
                .parse()
                .unwrap();
            Ok(height)
        }

        pub async fn query(&self, from_block: u64, to_block: u64) -> Result<Vec<Block>, reqwest::Error> {
            let worker_url = self.client.get(format!("https://v2.archive.subsquid.io/network/ethereum-mainnet/{}/worker", from_block))
                .send()
                .await?
                .text()
                .await?;
            let blocks: Vec<Block> = self.client.post(worker_url)
                .json(&json!({
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "fields": {
                        "block": {
                            "number": true,
                            "hash": true,
                            "parentHash": true,
                            "difficulty": true,
                            "totalDifficulty": true,
                            "size": true,
                            "sha3Uncles": true,
                            "gasLimit": true,
                            "gasUsed": true,
                            "timestamp": true,
                            "miner": true,
                            "stateRoot": true,
                            "transactionsRoot": true,
                            "receiptsRoot": true,
                            "logsBloom": true,
                            "extraData": true,
                            "mixHash": true,
                            "baseFeePerGas": true,
                            "nonce": true
                        }
                    }
                }))
                .send()
                .await?
                .json()
                .await?;
            Ok(blocks)
        }
    }
}

async fn resolve_negative_start_block_num(start_block_num: i64, archive: &archive::Archive) -> u64 {
    if start_block_num < 0 {
        let delta = u32::try_from(start_block_num.abs()).unwrap();
        let head = archive.height().await.unwrap();
        return head.saturating_sub(delta).into();
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
    archive: std::sync::Arc<archive::Archive>,
}

#[tonic::async_trait]
impl Stream for ArchiveStream {
    type BlocksStream = tokio_stream::wrappers::ReceiverStream<Result<firehose::Response, Status>>;

    async fn blocks(&self, request: Request<firehose::Request>) -> Result<Response<Self::BlocksStream>, Status> {
        let mut filters = vec![];
        for transform in &request.get_ref().transforms {
            let filter = transforms::CombinedFilter::decode(&transform.value[..]).unwrap();
            filters.push(filter);
        }
        let from_block = resolve_negative_start_block_num(request.get_ref().start_block_num, &self.archive).await;
        dbg!(request.get_ref());
        dbg!(&filters);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let archive = self.archive.clone();
        tokio::spawn(async move {
            loop {
                let height = archive.height().await.unwrap();
                let blocks = archive.query(from_block, request.get_ref().stop_block_num).await.unwrap();
                dbg!(&blocks);
                let last_block_num = blocks[blocks.len() - 1].header.number;
                for block in blocks {
                    let graph_block = Block {
                        ver: 2,
                        hash: prefix_hex::decode(block.header.hash.clone()).unwrap(),
                        number: block.header.number.into(),
                        size: block.header.size.into(),
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
                            number: block.header.number.into(),
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
                                .and_then(|val| Some(codec::BigInt { bytes: prefix_hex::decode(val).unwrap() })),
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
                        cursor: "cursor".to_string()
                    })).await.unwrap();
                }

                if height <= last_block_num {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:13042".parse()?;
    let stream = ArchiveStream { archive: std::sync::Arc::new(archive::Archive::new()) };
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
