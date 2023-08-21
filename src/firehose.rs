use crate::archive::{
    Archive, BatchRequest, BlockFieldSelection, CallType, FieldSelection, Log, LogFieldSelection,
    LogRequest, Trace, TraceFieldSelection, TraceType, TxFieldSelection,
};
use crate::pbcodec;
use crate::pbfirehose::single_block_request::Reference;
use crate::pbfirehose::{Request, Response, SingleBlockRequest, SingleBlockResponse};
use crate::pbtransforms::CombinedFilter;
use async_stream::stream;
use futures_core::stream::Stream;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
pub struct Firehose {
    archive: Arc<Archive>,
}

impl Firehose {
    pub fn new(archive: Arc<Archive>) -> Firehose {
        Firehose { archive }
    }

    pub async fn blocks(&self, request: Request) -> Result<impl Stream<Item = Response>, ()> {
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
            trace: None,
        };
        let mut logs: Vec<LogRequest> = vec![];

        for transform in &request.transforms {
            let filter = CombinedFilter::decode(&transform.value[..]).unwrap();
            for log_filter in filter.log_filters {
                let log_request = LogRequest {
                    address: log_filter
                        .addresses
                        .into_iter()
                        .map(|address| prefix_hex::encode(address))
                        .collect(),
                    topic0: log_filter
                        .event_signatures
                        .into_iter()
                        .map(|signature| prefix_hex::encode(signature))
                        .collect(),
                    topic1: vec![],
                    topic2: vec![],
                    topic3: vec![],
                    transaction: true,
                    transaction_traces: true,
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
                });
                fields.trace = Some(TraceFieldSelection {
                    transaction_index: true,
                    r#type: true,
                    revert_reason: true,
                    error: true,
                    create_from: true,
                    create_value: true,
                    create_gas: true,
                    create_result_gas_used: true,
                    create_result_address: true,
                    call_from: true,
                    call_to: true,
                    call_value: true,
                    call_gas: true,
                    call_input: true,
                    call_type: true,
                    call_result_gas_used: true,
                    call_result_output: true,
                });
            }
        }

        let from_block =
            resolve_negative_start_block_num(request.start_block_num, &self.archive).await;
        let to_block = if from_block != request.stop_block_num && request.stop_block_num == 0 {
            None
        } else {
            Some(request.stop_block_num)
        };
        let mut req = BatchRequest {
            from_block,
            to_block,
            fields: Some(fields),
            logs: None,
        };

        if !logs.is_empty() {
            req.logs = Some(logs);
        }

        let archive = self.archive.clone();
        Ok(stream! {
            'outer: loop {
                let height = archive.height().await.unwrap();

                if height <= req.from_block {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                let blocks = archive.query(&req).await.unwrap();
                let last_block_num = blocks[blocks.len() - 1].header.number;
                for block in blocks {
                    let mut graph_block = pbcodec::Block {
                        ver: 2,
                        hash: prefix_hex::decode(block.header.hash.clone()).unwrap(),
                        number: block.header.number,
                        size: block.header.size,
                        header: Some(pbcodec::BlockHeader {
                            parent_hash: prefix_hex::decode(block.header.parent_hash).unwrap(),
                            uncle_hash: prefix_hex::decode(block.header.sha3_uncles).unwrap(),
                            coinbase: prefix_hex::decode(block.header.miner).unwrap(),
                            state_root: prefix_hex::decode(block.header.state_root).unwrap(),
                            transactions_root: prefix_hex::decode(block.header.transactions_root)
                                .unwrap(),
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
                            gas_used: u64::from_str_radix(
                                &block.header.gas_used.trim_start_matches("0x"),
                                16,
                            )
                            .unwrap(),
                            timestamp: Some(prost_types::Timestamp {
                                seconds: i64::try_from(block.header.timestamp).unwrap(),
                                nanos: 0,
                            }),
                            extra_data: prefix_hex::decode(block.header.extra_data).unwrap(),
                            mix_hash: prefix_hex::decode(block.header.mix_hash).unwrap(),
                            nonce: u64::from_str_radix(
                                &block.header.nonce.trim_start_matches("0x"),
                                16,
                            )
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

                    let mut logs_by_tx: HashMap<u32, Vec<Log>> = HashMap::new();
                    if let Some(logs) = block.logs {
                        for log in logs {
                            if logs_by_tx.contains_key(&log.transaction_index) {
                                logs_by_tx.get_mut(&log.transaction_index)
                                    .unwrap()
                                    .push(log);
                            } else {
                                logs_by_tx.insert(log.transaction_index, vec![log]);
                            }
                        }
                    }

                    let mut traces_by_tx: HashMap<u32, Vec<Trace>> = HashMap::new();
                    if let Some(traces) = block.traces {
                        for trace in traces {
                            if traces_by_tx.contains_key(&trace.transaction_index) {
                                traces_by_tx.get_mut(&trace.transaction_index)
                                    .unwrap()
                                    .push(trace);
                            } else {
                                traces_by_tx.insert(trace.transaction_index, vec![trace]);
                            }
                        }
                    }

                    if let Some(transactions) = block.transactions {
                        graph_block.transaction_traces = transactions.into_iter().map(|tx| {
                            let logs = logs_by_tx.remove(&tx.transaction_index).unwrap_or_default().into_iter().map(|log| pbcodec::Log {
                                address: prefix_hex::decode(log.address).unwrap(),
                                data: prefix_hex::decode(log.data).unwrap(),
                                block_index: log.log_index,
                                topics: log.topics.into_iter().map(|topic| prefix_hex::decode(topic).unwrap()).collect(),
                                index: log.transaction_index,
                                ordinal: 0,
                            }).collect();
                            let calls = traces_by_tx.remove(&tx.transaction_index).unwrap_or_default().into_iter().filter_map(|trace| {
                                let call_type = match trace.r#type {
                                    TraceType::Create => 5,
                                    TraceType::Call => match trace.call_type.unwrap() {
                                        CallType::Call => 1,
                                        CallType::Callcode => 2,
                                        CallType::Delegatecall => 3,
                                        CallType::Staticcall => 4,
                                    },
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let caller = match trace.r#type {
                                    TraceType::Create => trace.create_from.unwrap(),
                                    TraceType::Call => trace.call_from.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let address = match trace.r#type {
                                    TraceType::Create => trace.create_result_address.unwrap(),
                                    TraceType::Call => trace.call_to.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let value = match trace.r#type {
                                    TraceType::Create => trace.create_value.unwrap(),
                                    TraceType::Call => trace.call_value.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let gas = match trace.r#type {
                                    TraceType::Create => trace.create_gas.unwrap(),
                                    TraceType::Call => trace.call_gas.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let gas_used = match trace.r#type {
                                    TraceType::Create => trace.create_result_gas_used.unwrap(),
                                    TraceType::Call => trace.call_result_gas_used.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let output = match trace.r#type {
                                    TraceType::Create => "0x".to_string(),
                                    TraceType::Call => trace.call_result_output.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                let input = match trace.r#type {
                                    TraceType::Create => "0x".to_string(),
                                    TraceType::Call => trace.call_input.unwrap(),
                                    TraceType::Suicide | TraceType::Reward => return None,
                                };
                                Some(pbcodec::Call {
                                    index: 0,
                                    parent_index: 0,
                                    depth: 0,
                                    call_type,
                                    caller: vec_from_hex(&caller).unwrap(),
                                    address: vec_from_hex(&address).unwrap(),
                                    value: Some(pbcodec::BigInt { bytes: vec_from_hex(&value).unwrap() }),
                                    gas_limit: gas.parse().unwrap(),
                                    gas_consumed: gas_used.parse().unwrap(),
                                    return_data: vec_from_hex(&output).unwrap(),
                                    input: vec_from_hex(&input).unwrap(),
                                    executed_code: false,
                                    suicide: false,
                                    keccak_preimages: HashMap::new(),
                                    storage_changes: vec![],
                                    balance_changes: vec![],
                                    nonce_changes: vec![],
                                    logs: vec![],
                                    code_changes: vec![],
                                    gas_changes: vec![],
                                    status_failed: trace.error.is_some() || trace.revert_reason.is_some(),
                                    status_reverted: trace.revert_reason.is_some(),
                                    failure_reason: trace.error.unwrap_or_else(|| trace.revert_reason.unwrap_or_default()),
                                    state_reverted: false,
                                    begin_ordinal: 0,
                                    end_ordinal: 0,
                                    account_creations: vec![],
                                })
                            }).collect();
                            pbcodec::TransactionTrace {
                                to: prefix_hex::decode(tx.to.unwrap_or("0x".to_string())).unwrap(),
                                nonce: tx.nonce,
                                gas_price: Some(pbcodec::BigInt { bytes: vec_from_hex(&tx.gas_price).unwrap() }),
                                gas_limit: u64::from_str_radix(&tx.gas.trim_start_matches("0x"), 16).unwrap(),
                                gas_used: u64::from_str_radix(&tx.gas_used.trim_start_matches("0x"), 16).unwrap(),
                                value: Some(pbcodec::BigInt { bytes: vec_from_hex(&tx.value).unwrap() }),
                                input: prefix_hex::decode(tx.input).unwrap(),
                                v: vec_from_hex(&tx.v).unwrap(),
                                r: vec_from_hex(&tx.r).unwrap(),
                                s: vec_from_hex(&tx.s).unwrap(),
                                r#type: tx.r#type,
                                access_list: vec![],
                                max_fee_per_gas: tx.max_fee_per_gas
                                    .and_then(|val| Some(pbcodec::BigInt { bytes: vec_from_hex(&val).unwrap() })),
                                max_priority_fee_per_gas: tx.max_priority_fee_per_gas
                                    .and_then(|val| Some(pbcodec::BigInt { bytes: vec_from_hex(&val).unwrap() })),
                                index: tx.transaction_index,
                                hash: prefix_hex::decode(tx.hash).unwrap(),
                                from: prefix_hex::decode(tx.from).unwrap(),
                                return_data: vec![],
                                public_key: vec![],
                                begin_ordinal: 0,
                                end_ordinal: 0,
                                status: tx.status,
                                receipt: Some(pbcodec::TransactionReceipt {
                                    state_root: vec![],
                                    cumulative_gas_used: u64::from_str_radix(&tx.cumulative_gas_used.trim_start_matches("0x"), 16).unwrap(),
                                    logs_bloom: prefix_hex::decode("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap(),
                                    logs,
                                }),
                                calls,
                            }
                        })
                        .collect()
                    }

                    yield Response {
                        block: Some(prost_types::Any {
                            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                            value: graph_block.encode_to_vec(),
                        }),
                        step: 1,
                        cursor: graph_block.number.to_string(),
                    };
                }

                if let Some(to_block) = to_block {
                    if last_block_num == to_block {
                        break 'outer;
                    }
                }

                req.from_block = last_block_num + 1;
            }
        })
    }

    pub async fn block(&self, request: SingleBlockRequest) -> Result<SingleBlockResponse, ()> {
        let block_num = match request.reference.as_ref().unwrap() {
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
                trace: None,
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

        Ok(SingleBlockResponse {
            block: Some(prost_types::Any {
                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                value: graph_block.encode_to_vec(),
            }),
        })
    }
}
