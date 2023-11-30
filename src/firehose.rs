use crate::cursor::Cursor;
use crate::datasource::{
    Block, BlockHeader, CallType, DataRequest, DataSource, HashAndHeight, HotDataSource, Log,
    LogRequest, Trace, TraceResult, TraceType, Transaction, TraceRequest,
};
use crate::pbcodec;
use crate::pbfirehose::single_block_request::Reference;
use crate::pbfirehose::{ForkStep, Request, Response, SingleBlockRequest, SingleBlockResponse};
use crate::pbtransforms::CombinedFilter;
use anyhow::{format_err, Context};
use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::stream::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

async fn resolve_negative_start(
    start_block_num: i64,
    ds: &(dyn DataSource + Send + Sync),
) -> anyhow::Result<u64> {
    if start_block_num < 0 {
        let delta = u64::try_from(start_block_num.abs())?;
        let head = ds.get_finalized_height().await?;
        return Ok(head.saturating_sub(delta));
    }
    Ok(u64::try_from(start_block_num)?)
}

fn try_decode_hex(label: &'static str, value: &str) -> anyhow::Result<Vec<u8>> {
    let buf: Vec<u8> = if value.len() % 2 != 0 {
        let value = format!("0x0{}", &value[2..]);
        prefix_hex::decode(&value).map_err(|_| format_err!("invalid {}: {}", label, value))?
    } else {
        prefix_hex::decode(value).map_err(|_| format_err!("invalid {}: {}", label, value))?
    };

    Ok(buf)
}

fn qty2int(value: &String) -> anyhow::Result<u64> {
    Ok(u64::from_str_radix(value.trim_start_matches("0x"), 16)?)
}

pub struct Firehose {
    archive: Arc<dyn DataSource + Sync + Send>,
    rpc: Option<Arc<dyn HotDataSource + Sync + Send>>,
}

impl Firehose {
    pub fn new(
        archive: Arc<dyn DataSource + Sync + Send>,
        rpc: Option<Arc<dyn HotDataSource + Sync + Send>>,
    ) -> Firehose {
        Firehose { archive, rpc }
    }

    pub async fn blocks(
        &self,
        request: Request,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Response>>> {
        let from_block = if request.cursor.is_empty() {
            if let Some(rpc) = &self.rpc {
                resolve_negative_start(request.start_block_num, rpc.as_ds()).await?
            } else {
                resolve_negative_start(request.start_block_num, &*self.archive).await?
            }
        } else {
            let cursor = Cursor::try_from(&request.cursor).map_err(|e| anyhow::anyhow!(e))?;
            cursor.block.height + 1
        };

        let to_block = if request.stop_block_num == 0 {
            None
        } else {
            Some(request.stop_block_num)
        };

        let mut logs: Vec<LogRequest> = vec![];
        let mut traces: Vec<TraceRequest> = vec![];
        for transform in &request.transforms {
            let filter = CombinedFilter::decode(&transform.value[..])?;

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
                };
                logs.push(log_request);
            }

            for call_filter in filter.call_filters {
                let trace_request = TraceRequest {
                    address: call_filter
                        .addresses
                        .into_iter()
                        .map(|address| prefix_hex::encode(address))
                        .collect(),
                    sighash: call_filter
                        .signatures
                        .into_iter()
                        .map(|signature| prefix_hex::encode(signature))
                        .collect(),
                };
                traces.push(trace_request);
            }
        }

        let archive = self.archive.clone();
        let rpc = self.rpc.clone();

        Ok(try_stream! {
            let mut state = None;
            let mut from_block = from_block;

            let archive_height = archive.get_finalized_height().await?;
            if from_block < archive_height || rpc.is_none() {
                let req = DataRequest {
                    from: from_block,
                    to: to_block,
                    logs: logs.clone(),
                    traces: traces.clone(),
                };
                let mut stream = Pin::from(archive.get_finalized_blocks(req, rpc.is_some())?);
                while let Some(result) = stream.next().await {
                    let blocks = result?;
                    for block in blocks {
                        let cursor = Cursor::new((&block).into(), (&block).into());
                        state = Some(HashAndHeight {
                            hash: block.header.hash.clone(),
                            height: block.header.number,
                        });
                        from_block = block.header.number + 1;

                        let graph_block = pbcodec::Block::try_from(block)?;

                        yield Response {
                            block: Some(prost_types::Any {
                                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                                value: graph_block.encode_to_vec(),
                            }),
                            step: ForkStep::StepNew.into(),
                            cursor: cursor.to_string(),
                        };
                    }
                }

                if let Some(to_block) = to_block {
                    if state.as_ref().unwrap().height == to_block {
                        return
                    }
                }
            }

            let rpc = if let Some(rpc) = rpc {
                rpc
            } else {
                return
            };

            let rpc_height = rpc.get_finalized_height().await?;
            if from_block < rpc_height {
                let to = if let Some(to_block) = to_block {
                    std::cmp::min(to_block, rpc_height)
                } else {
                    rpc_height
                };
                let req = DataRequest {
                    from: from_block,
                    to: Some(to),
                    logs: logs.clone(),
                    traces: traces.clone(),
                };
                let mut stream = Pin::from(rpc.get_finalized_blocks(req, true)?);
                while let Some(result) = stream.next().await {
                    let blocks = result?;
                    for block in blocks {
                        let cursor = Cursor::new((&block).into(), (&block).into());
                        let graph_block = pbcodec::Block::try_from(block)?;

                        yield Response {
                            block: Some(prost_types::Any {
                                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                                value: graph_block.encode_to_vec(),
                            }),
                            step: ForkStep::StepNew.into(),
                            cursor: cursor.to_string(),
                        };
                    }
                }
                state = Some(HashAndHeight {
                    hash: rpc.get_block_hash(to).await?,
                    height: to,
                });
                from_block = to + 1;

                if let Some(to_block) = to_block {
                    if state.as_ref().unwrap().height == to_block {
                        return
                    }
                }
            }

            let req = DataRequest {
                from: from_block,
                to: to_block,
                logs,
                traces,
            };
            let state = state.context("state isn't expected to be None")?;
            let mut last_head = state.clone();
            let mut stream = Pin::from(rpc.get_hot_blocks(req, state)?);
            while let Some(result) = stream.next().await {
                let upd = result?;

                let new_head = if upd.blocks.is_empty() {
                    upd.base_head.clone()
                } else {
                    let header = &upd.blocks.last().unwrap().header;
                    HashAndHeight {
                        hash: header.hash.clone(),
                        height: header.number,
                    }
                };

                if upd.base_head != last_head {
                    // fork happened
                    // only number and parent_hash are required for ForkStep::StepUndo
                    let cursor = Cursor::new(upd.base_head.clone(), upd.finalized_head.clone());
                    let mut graph_block = pbcodec::Block::default();
                    let mut header = pbcodec::BlockHeader::default();
                    header.number = last_head.height;
                    header.parent_hash = prefix_hex::decode(upd.base_head.hash)?;
                    graph_block.header = Some(header);

                    yield Response {
                        block: Some(prost_types::Any {
                            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                            value: graph_block.encode_to_vec(),
                        }),
                        step: ForkStep::StepUndo.into(),
                        cursor: cursor.to_string(),
                    };
                }

                for block in upd.blocks {
                    let cursor = Cursor::new((&block).into(), upd.finalized_head.clone());
                    let graph_block = pbcodec::Block::try_from(block)?;
                    yield Response {
                        block: Some(prost_types::Any {
                            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                            value: graph_block.encode_to_vec(),
                        }),
                        step: ForkStep::StepNew.into(),
                        cursor: cursor.to_string(),
                    }
                }

                last_head = new_head;
            }
        })
    }

    pub async fn block(&self, request: SingleBlockRequest) -> anyhow::Result<SingleBlockResponse> {
        let block_num = match request.reference.as_ref().unwrap() {
            Reference::BlockNumber(block_number) => block_number.num,
            Reference::BlockHashAndNumber(block_hash_and_number) => block_hash_and_number.num,
            Reference::Cursor(cursor) => {
                let cursor = Cursor::try_from(&cursor.cursor).unwrap();
                cursor.block.height
            }
        };

        let req = DataRequest {
            from: block_num,
            to: Some(block_num),
            logs: vec![],
            traces: vec![],
        };

        let mut stream = Pin::from(self.archive.get_finalized_blocks(req, true)?);
        let blocks = stream.next().await.unwrap()?;
        let block = blocks.into_iter().nth(0).unwrap();

        let graph_block = pbcodec::Block::try_from(block)?;

        Ok(SingleBlockResponse {
            block: Some(prost_types::Any {
                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".to_string(),
                value: graph_block.encode_to_vec(),
            }),
        })
    }
}

impl TryFrom<BlockHeader> for pbcodec::BlockHeader {
    type Error = anyhow::Error;

    fn try_from(value: BlockHeader) -> anyhow::Result<Self, Self::Error> {
        Ok(pbcodec::BlockHeader {
            parent_hash: try_decode_hex("parent hash", &value.parent_hash)?,
            uncle_hash: try_decode_hex("sha3 uncles", &value.sha3_uncles)?,
            coinbase: try_decode_hex("miner", &value.miner)?,
            state_root: try_decode_hex("state root", &value.state_root)?,
            transactions_root: try_decode_hex("transactions root", &value.transactions_root)?,
            receipt_root: try_decode_hex("receipts root", &value.receipts_root)?,
            logs_bloom: try_decode_hex("logs bloom", &value.logs_bloom)?,
            difficulty: Some(pbcodec::BigInt {
                bytes: try_decode_hex("difficulty", &value.difficulty)?,
            }),
            total_difficulty: Some(pbcodec::BigInt {
                bytes: try_decode_hex("total difficulty", &value.total_difficulty)?,
            }),
            number: value.number,
            gas_limit: qty2int(&value.gas_limit)?,
            gas_used: qty2int(&value.gas_used)?,
            timestamp: Some(prost_types::Timestamp {
                seconds: i64::try_from(value.timestamp)?,
                nanos: 0,
            }),
            extra_data: try_decode_hex("extra data", &value.extra_data)?,
            mix_hash: try_decode_hex("mix hash", &value.mix_hash)?,
            nonce: qty2int(&value.nonce)?,
            hash: try_decode_hex("hash", &value.hash)?,
            base_fee_per_gas: value.base_fee_per_gas.map_or::<anyhow::Result<_>, _>(
                Ok(None),
                |val| {
                    Ok(Some(pbcodec::BigInt {
                        bytes: try_decode_hex("base fee per gas", &val)?,
                    }))
                },
            )?,
        })
    }
}

impl TryFrom<Transaction> for pbcodec::TransactionTrace {
    type Error = anyhow::Error;

    fn try_from(value: Transaction) -> Result<Self, Self::Error> {
        Ok(pbcodec::TransactionTrace {
            to: try_decode_hex(
                "tx to",
                &value
                    .to
                    .unwrap_or("0x0000000000000000000000000000000000000000".to_string()),
            )?,
            nonce: value.nonce,
            gas_price: Some(pbcodec::BigInt {
                bytes: try_decode_hex("tx gas price", &value.gas_price)?,
            }),
            gas_limit: qty2int(&value.gas)?,
            gas_used: qty2int(&value.gas_used)?,
            value: Some(pbcodec::BigInt {
                bytes: try_decode_hex("tx value", &value.value)?,
            }),
            input: try_decode_hex("tx input", &value.input)?,
            v: try_decode_hex("tx v", &value.v)?,
            r: try_decode_hex("tx r", &value.r)?,
            s: try_decode_hex("tx s", &value.s)?,
            r#type: value.r#type,
            access_list: vec![],
            max_fee_per_gas: value.max_fee_per_gas.map_or::<anyhow::Result<_>, _>(
                Ok(None),
                |val| {
                    Ok(Some(pbcodec::BigInt {
                        bytes: try_decode_hex("tx max fee", &val)?,
                    }))
                },
            )?,
            max_priority_fee_per_gas: value
                .max_priority_fee_per_gas
                .map_or::<anyhow::Result<_>, _>(Ok(None), |val| {
                    Ok(Some(pbcodec::BigInt {
                        bytes: try_decode_hex("tx max priority", &val)?,
                    }))
                })?,
            index: value.transaction_index,
            hash: try_decode_hex("tx hash", &value.hash)?,
            from: try_decode_hex("tx from", &value.from)?,
            return_data: vec![],
            public_key: vec![],
            begin_ordinal: 0,
            end_ordinal: 0,
            status: pbcodec::TransactionTraceStatus::Unknown.into(),
            receipt: None,
            calls: vec![],
        })
    }
}

impl TryFrom<Trace> for pbcodec::Call {
    type Error = anyhow::Error;

    fn try_from(value: Trace) -> Result<Self, Self::Error> {
        match value.r#type {
            TraceType::Create => {
                let action = value.action.context("no action")?;
                let result = value.result.context("no result")?;
                let gas = action.gas.context("no gas")?;
                let gas_used = result.gas_used.context("no gas_used")?;

                Ok(pbcodec::Call {
                    call_type: 5,
                    caller: try_decode_hex("trace from", &action.from.context("no from")?)?,
                    address: try_decode_hex(
                        "trace address",
                        &result.address.context("no address")?,
                    )?,
                    value: action
                        .value
                        .map_or::<anyhow::Result<_>, _>(Ok(None), |val| {
                            Ok(Some(pbcodec::BigInt {
                                bytes: try_decode_hex("trace value", &val)?,
                            }))
                        })?,
                    gas_limit: u64::from_str_radix(&gas.trim_start_matches("0x"), 16)?,
                    gas_consumed: u64::from_str_radix(&gas_used.trim_start_matches("0x"), 16)?,
                    return_data: prefix_hex::decode("0x")?,
                    input: prefix_hex::decode("0x")?,
                    status_failed: value.error.is_some() || value.revert_reason.is_some(),
                    status_reverted: value.revert_reason.is_some(),
                    failure_reason: value
                        .error
                        .unwrap_or_else(|| value.revert_reason.unwrap_or_default()),
                    ..Default::default()
                })
            }
            TraceType::Call => {
                let action = value.action.context("no action")?;
                let result = value.result.unwrap_or(TraceResult {
                    gas_used: None,
                    address: None,
                    output: None,
                });
                let call_type = match action.r#type.context("no type")? {
                    CallType::Call => 1,
                    CallType::Callcode => 2,
                    CallType::Delegatecall => 3,
                    CallType::Staticcall => 4,
                };
                let gas = action.gas.context("no gas")?;
                let gas_used = result.gas_used.unwrap_or("0x0".to_string());
                let output = result.output.unwrap_or("0x".to_string());

                Ok(pbcodec::Call {
                    call_type,
                    caller: try_decode_hex("trace from", &action.from.context("no from")?)?,
                    address: try_decode_hex("trace to", &action.to.context("no to")?)?,
                    value: action
                        .value
                        .map_or::<anyhow::Result<_>, _>(Ok(None), |val| {
                            Ok(Some(pbcodec::BigInt {
                                bytes: try_decode_hex("trace value", &val)?,
                            }))
                        })?,
                    gas_limit: u64::from_str_radix(&gas.trim_start_matches("0x"), 16)?,
                    gas_consumed: u64::from_str_radix(&gas_used.trim_start_matches("0x"), 16)?,
                    return_data: try_decode_hex("trace output", &output)?,
                    input: try_decode_hex("trace input", &action.input.context("no input")?)?,
                    status_failed: value.error.is_some() || value.revert_reason.is_some(),
                    status_reverted: value.revert_reason.is_some(),
                    failure_reason: value
                        .error
                        .unwrap_or_else(|| value.revert_reason.unwrap_or_default()),
                    ..Default::default()
                })
            }
            TraceType::Suicide | TraceType::Reward => anyhow::bail!("unsupported trace type"),
        }
    }
}

fn get_tx_trace_status(calls: &Vec<pbcodec::Call>) -> i32 {
    let call = &calls[0];
    if call.status_failed && call.state_reverted {
        pbcodec::TransactionTraceStatus::Reverted.into()
    } else if call.status_failed {
        pbcodec::TransactionTraceStatus::Failed.into()
    } else {
        pbcodec::TransactionTraceStatus::Succeeded.into()
    }
}

impl TryFrom<Block> for pbcodec::Block {
    type Error = anyhow::Error;

    fn try_from(value: Block) -> Result<Self, Self::Error> {
        let mut logs_by_tx: HashMap<u32, Vec<Log>> = HashMap::new();
        for log in value.logs {
            if logs_by_tx.contains_key(&log.transaction_index) {
                logs_by_tx
                    .get_mut(&log.transaction_index)
                    .unwrap()
                    .push(log);
            } else {
                logs_by_tx.insert(log.transaction_index, vec![log]);
            }
        }

        let mut traces_by_tx: HashMap<u32, Vec<Trace>> = HashMap::new();
        for trace in value.traces {
            if traces_by_tx.contains_key(&trace.transaction_index) {
                traces_by_tx
                    .get_mut(&trace.transaction_index)
                    .unwrap()
                    .push(trace);
            } else {
                traces_by_tx.insert(trace.transaction_index, vec![trace]);
            }
        }

        let transaction_traces = value.transactions.into_iter().map(|tx| {
            let logs = logs_by_tx.remove(&tx.transaction_index).unwrap_or_default().into_iter().map(|log| pbcodec::Log {
                address: try_decode_hex("log address", &log.address).unwrap(),
                data: try_decode_hex("log data", &log.data).unwrap(),
                block_index: log.log_index,
                topics: log.topics.into_iter().map(|topic| try_decode_hex("log topic", &topic).unwrap()).collect(),
                index: log.transaction_index,
                ordinal: 0,
            }).collect();
            let calls = traces_by_tx.remove(&tx.transaction_index).unwrap_or_default().into_iter().filter_map(|trace| {
                match trace.r#type {
                    TraceType::Call | TraceType::Create => Some(pbcodec::Call::try_from(trace)),
                    TraceType::Reward | TraceType::Suicide => None,
                }
            }).collect::<anyhow::Result<Vec<pbcodec::Call>>>()?;
            let receipt = pbcodec::TransactionReceipt {
                state_root: vec![],
                cumulative_gas_used: qty2int(&tx.cumulative_gas_used)?,
                logs_bloom: prefix_hex::decode("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")?,
                logs,
            };
            let mut tx_trace = pbcodec::TransactionTrace::try_from(tx)?;
            tx_trace.status = get_tx_trace_status(&calls);
            tx_trace.receipt = Some(receipt);
            tx_trace.calls = calls;
            Ok(tx_trace)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(pbcodec::Block {
            ver: 2,
            hash: try_decode_hex("hash", &value.header.hash.clone())?,
            number: value.header.number,
            size: value.header.size,
            header: Some(pbcodec::BlockHeader::try_from(value.header)?),
            uncles: vec![],
            transaction_traces,
            balance_changes: vec![],
            code_changes: vec![],
        })
    }
}
