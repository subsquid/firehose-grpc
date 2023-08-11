use crate::archive::{Archive, BatchRequest, BlockFieldSelection, FieldSelection};
use crate::pbcodec;
use crate::pbfirehose::single_block_request::Reference;
use crate::pbfirehose::{fetch_server::Fetch, SingleBlockRequest, SingleBlockResponse};
use prost::Message;
use std::sync::Arc;

fn vec_from_hex(value: &str) -> Result<Vec<u8>, prefix_hex::Error> {
    let buf: Vec<u8> = if value.len() % 2 != 0 {
        let value = format!("0x0{}", &value[2..]);
        prefix_hex::decode(value)?
    } else {
        prefix_hex::decode(value)?
    };

    Ok(buf)
}

pub struct ArchiveFetch {
    pub archive: Arc<Archive>,
}

#[tonic::async_trait]
impl Fetch for ArchiveFetch {
    async fn block(
        &self,
        request: tonic::Request<SingleBlockRequest>,
    ) -> Result<tonic::Response<SingleBlockResponse>, tonic::Status> {
        dbg!(&request);
        let block_num = match request.get_ref().reference.as_ref().unwrap() {
            Reference::BlockNumber(block_number) => block_number.num,
            Reference::BlockHashAndNumber(block_hash_and_number) => block_hash_and_number.num,
            Reference::Cursor(cursor) => cursor.cursor.parse().unwrap(),
        };
        let req = BatchRequest {
            from_block: block_num,
            to_block: Some(block_num),
            fields: Some(FieldSelection {
                block: Some(BlockFieldSelection {
                    base_fee_per_gas: true,
                    difficulty: true,
                    total_difficulty: true,
                    extra_data: true,
                    gas_limit: true,
                    gas_used: true,
                    hash: true,
                    logs_bloom: true,
                    miner: true,
                    mix_hash: true,
                    nonce: true,
                    number: true,
                    parent_hash: true,
                    receipts_root: true,
                    sha3_uncles: true,
                    size: true,
                    state_root: true,
                    timestamp: true,
                    transactions_root: true,
                }),
                log: None,
                transaction: None,
            }),
            logs: None,
        };
        let blocks = self.archive.query(&req).await.unwrap();
        let block = blocks.into_iter().nth(0).unwrap();
        let graph_block = pbcodec::Block {
            ver: 2,
            hash: prefix_hex::decode(block.header.hash.clone()).unwrap(),
            number: block.header.number,
            size: block.header.size,
            header: Some(pbcodec::BlockHeader {
                parent_hash: prefix_hex::decode(block.header.parent_hash).unwrap(),
                uncle_hash: prefix_hex::decode(block.header.sha3_uncles).unwrap(),
                coinbase: prefix_hex::decode(block.header.miner).unwrap(),
                state_root: prefix_hex::decode(block.header.state_root).unwrap(),
                transactions_root: prefix_hex::decode(block.header.transactions_root).unwrap(),
                receipt_root: prefix_hex::decode(block.header.receipts_root).unwrap(),
                logs_bloom: prefix_hex::decode(block.header.logs_bloom).unwrap(),
                difficulty: Some(pbcodec::BigInt {
                    bytes: vec_from_hex(&block.header.difficulty).unwrap(),
                }),
                total_difficulty: Some(pbcodec::BigInt {
                    bytes: vec_from_hex(&block.header.total_difficulty).unwrap(),
                }),
                number: block.header.number,
                gas_limit: u64::from_str_radix(
                    &block.header.gas_limit.trim_start_matches("0x"),
                    16,
                )
                .unwrap(),
                gas_used: u64::from_str_radix(&block.header.gas_used.trim_start_matches("0x"), 16)
                    .unwrap(),
                timestamp: Some(prost_types::Timestamp {
                    seconds: i64::try_from(block.header.timestamp).unwrap(),
                    nanos: 0,
                }),
                extra_data: prefix_hex::decode(block.header.extra_data).unwrap(),
                mix_hash: prefix_hex::decode(block.header.mix_hash).unwrap(),
                nonce: u64::from_str_radix(&block.header.nonce.trim_start_matches("0x"), 16)
                    .unwrap(),
                hash: prefix_hex::decode(block.header.hash).unwrap(),
                base_fee_per_gas: block.header.base_fee_per_gas.and_then(|val| {
                    Some(pbcodec::BigInt {
                        bytes: vec_from_hex(&val).unwrap(),
                    })
                }),
            }),
            uncles: vec![],
            transaction_traces: vec![],
            balance_changes: vec![],
            code_changes: vec![],
        };

        Ok(tonic::Response::new(SingleBlockResponse {
            block: Some(prost_types::Any {
                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                value: graph_block.encode_to_vec(),
            }),
        }))
    }
}
