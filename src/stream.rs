use tokio_stream::wrappers::ReceiverStream;
use std::time::Duration;
use std::sync::Arc;
use std::collections::HashMap;
use prost::Message;
use crate::archive::{
    Archive, BatchRequest, FieldSelection, BlockFieldSelection, LogFieldSelection, TxFieldSelection,
    LogRequest, Log,
};
use crate::codec;
use crate::firehose::{stream_server::Stream, Request, Response};
use crate::transforms::CombinedFilter;

async fn resolve_negative_start_block_num(start_block_num: i64, archive: &Archive) -> u64 {
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
    pub archive: Arc<Archive>,
}

#[tonic::async_trait]
impl Stream for ArchiveStream {
    type BlocksStream = ReceiverStream<Result<Response, tonic::Status>>;

    async fn blocks(&self, request: tonic::Request<Request>) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status> {
        let mut fields = FieldSelection {
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
        };
        let mut logs: Vec<LogRequest> = vec![];

        for transform in &request.get_ref().transforms {
            let filter = CombinedFilter::decode(&transform.value[..]).unwrap();
            for log_filter in filter.log_filters {
                let log_request = LogRequest {
                    address: log_filter.addresses.into_iter().map(|address| prefix_hex::encode(address)).collect(),
                    topic0: log_filter.event_signatures.into_iter().map(|signature| prefix_hex::encode(signature)).collect(),
                    topic1: vec![],
                    topic2: vec![],
                    topic3: vec![],
                    transaction: true,
                };
                logs.push(log_request);
                fields.log = Some(LogFieldSelection {
                    address: true,
                    data: true,
                    log_index: true,
                    topics: true,
                    transaction_index: true,
                });
                fields.transaction = Some(TxFieldSelection {
                    cumulative_gas_used: true,
                    effective_gas_price: true,
                    from: true,
                    gas: true,
                    gas_price: true,
                    gas_used: true,
                    input: true,
                    max_fee_per_gas: true,
                    max_priority_fee_per_gas: true,
                    nonce: true,
                    r: true,
                    s: true,
                    hash: true,
                    status: true,
                    to: true,
                    transaction_index: true,
                    r#type: true,
                    v: true,
                    value: true,
                    y_parity: true,
                })
            }
        }

        let is_head_stream = request.get_ref().start_block_num == -1;
        let from_block = resolve_negative_start_block_num(request.get_ref().start_block_num, &self.archive).await;
        let to_block = if request.get_ref().stop_block_num == 0 {
            None
        } else {
            Some(request.get_ref().stop_block_num)
        };
        let mut req = BatchRequest {
            from_block,
            to_block,
            fields: Some(fields),
            logs: Some(logs),
        };
        let mut request_count = 0;
        dbg!(request.get_ref());
        dbg!(&req);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let archive = self.archive.clone();
        tokio::spawn(async move {
            'outer: loop {
                request_count += 1;
                let height = archive.height().await.unwrap();
                let blocks = archive.query(&req).await.unwrap();
                let last_block_num = blocks[blocks.len() - 1].header.number;
                for block in blocks {
                    let mut graph_block = codec::Block {
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

                    let mut logs_by_tx: HashMap<u32, Vec<Log>> = HashMap::new();
                    if let Some(logs) = block.logs {
                        for log in logs {
                            if logs_by_tx.contains_key(&log.transaction_index) {
                                logs_by_tx.get_mut(&log.transaction_index).unwrap().push(log);
                            } else {
                                logs_by_tx.insert(log.transaction_index, vec![log]);
                            }
                        }
                    }

                    if let Some(transactions) = block.transactions {
                        graph_block.transaction_traces = transactions.into_iter().map(|tx| {
                            let logs = logs_by_tx.remove(&tx.transaction_index).unwrap_or_default().into_iter().map(|log| codec::Log {
                                address: prefix_hex::decode(log.address).unwrap(),
                                data: prefix_hex::decode(log.data).unwrap(),
                                block_index: log.log_index,
                                topics: log.topics.into_iter().map(|topic| prefix_hex::decode(topic).unwrap()).collect(),
                                index: log.transaction_index,
                                ordinal: 0,
                            }).collect();
                            codec::TransactionTrace {
                                to: prefix_hex::decode(tx.to).unwrap(),
                                nonce: tx.nonce,
                                gas_price: Some(codec::BigInt { bytes: vec_from_hex(&tx.gas_price).unwrap() }),
                                gas_limit: u64::from_str_radix(&tx.gas.trim_start_matches("0x"), 16).unwrap(),
                                gas_used: u64::from_str_radix(&tx.gas_used.trim_start_matches("0x"), 16).unwrap(),
                                value: Some(codec::BigInt { bytes: vec_from_hex(&tx.value).unwrap() }),
                                input: prefix_hex::decode(tx.input).unwrap(),
                                v: vec_from_hex(&tx.v).unwrap(),
                                r: vec_from_hex(&tx.r).unwrap(),
                                s: vec_from_hex(&tx.s).unwrap(),
                                r#type: tx.r#type,
                                access_list: vec![],
                                max_fee_per_gas: tx.max_fee_per_gas
                                    .and_then(|val| Some(codec::BigInt { bytes: vec_from_hex(&val).unwrap() })),
                                max_priority_fee_per_gas: tx.max_priority_fee_per_gas
                                    .and_then(|val| Some(codec::BigInt { bytes: vec_from_hex(&val).unwrap() })),
                                index: tx.transaction_index,
                                hash: prefix_hex::decode(tx.hash).unwrap(),
                                from: prefix_hex::decode(tx.from).unwrap(),
                                return_data: vec![],
                                public_key: vec![],
                                begin_ordinal: 0,
                                end_ordinal: 0,
                                status: tx.status,
                                receipt: Some(codec::TransactionReceipt {
                                    state_root: vec![],
                                    cumulative_gas_used: u64::from_str_radix(&tx.cumulative_gas_used.trim_start_matches("0x"), 16).unwrap(),
                                    logs_bloom: prefix_hex::decode("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                                    logs,
                                }),
                                calls: vec![],
                            }
                        })
                        .collect()
                    }

                    if !is_head_stream {
                        dbg!(format!(
                            "sending №{}, from: {} to: {} count: {}",
                            block.header.number,
                            request.get_ref().start_block_num,
                            request.get_ref().stop_block_num,
                            request_count,
                        ));
                    }
                    let result = tx.send(Ok(Response {
                        block: Some(prost_types::Any {
                            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                            value: graph_block.encode_to_vec(),
                        }),
                        step: 1,
                        cursor: request.get_ref().cursor.clone(),
                    })).await;

                    if let Err(_) = result {
                        break 'outer;
                    }
                }

                if let Some(to_block) = to_block {
                    if u64::from(last_block_num) == to_block {
                        break;
                    }
                }

                if height <= last_block_num {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    req.from_block = last_block_num + 1;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
