use crate::datasource::{
    Block, BlockHeader, BlockStream, CallType, DataRequest, DataSource, Log, Trace, TraceAction,
    TraceResult, TraceType, Transaction,
};
use crate::{
    portal,
    portal::{
        Portal, Query, BlockFieldSelection, FieldSelection, LogFieldSelection, LogRequest,
        TraceFieldSelection, TxFieldSelection, TraceRequest, TxRequest
    },
};
use async_stream::try_stream;
use serde_json::Number;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct PortalDataSource {
    portal: Arc<Portal>,
}

impl PortalDataSource {
    pub fn new(portal: Arc<Portal>) -> PortalDataSource {
        PortalDataSource { portal }
    }
}

#[async_trait::async_trait]
impl DataSource for PortalDataSource {
    async fn get_finalized_blocks(
        &self,
        request: DataRequest,
        stop_on_head: bool,
    ) -> anyhow::Result<BlockStream> {
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

        let logs = if request.logs.is_empty() {
            None
        } else {
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
                r#type: true,
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
            let logs = request
                .logs
                .into_iter()
                .map(|r| LogRequest {
                    address: r.address,
                    topic0: r.topic0,
                    transaction: r.transaction,
                    transaction_traces: r.transaction_traces,
                    transaction_logs: r.transaction_logs,
                })
                .collect();
            Some(logs)
        };

        let transactions = if request.transactions.is_empty() {
            None
        } else {
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
            let transactions = request
                .transactions
                .into_iter()
                .map(|r| TxRequest {
                    to: r.address,
                    sighash: r.sighash,
                    traces: r.traces,
                })
                .collect();
            Some(transactions)
        };

        let traces = if request.traces.is_empty() {
            None
        } else {
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
                r#type: true,
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
            let traces = request
                .traces
                .into_iter()
                .map(|r| TraceRequest {
                    call_to: r.address,
                    call_sighash: r.sighash,
                    transaction: r.transaction,
                    parents: r.parents,
                    transaction_logs: r.transaction_logs,
                })
                .collect();
            Some(traces)
        };

        let mut query = Query {
            from_block: request.from,
            to_block: request.to,
            fields: Some(fields),
            logs,
            transactions,
            traces,
        };

        let portal = self.portal.clone();
        Ok(Box::new(try_stream! {
            loop {
                let stream = portal.stream(&query).await?;
                for await block in stream {
                    let block = Block::from(block?);
                    let block_num = block.header.number;

                    yield vec![block];

                    if let Some(to_block) = query.to_block {
                        if block_num == to_block {
                            break;
                        }
                    }

                    query.from_block = block_num + 1
                }

                if stop_on_head {
                    break;
                } else {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
        }))
    }

    async fn get_finalized_height(&self) -> anyhow::Result<u64> {
        self.portal.height().await
    }

    async fn get_block_hash(&self, _height: u64) -> anyhow::Result<String> {
        todo!()
    }
}

fn to_u64(value: Number) -> u64 {
    if let Some(val) = value.as_u64() {
        return val;
    }
    if let Some(val) = value.as_f64() {
        return val as u64;
    }
    unimplemented!()
}

impl From<portal::BlockHeader> for BlockHeader {
    fn from(value: portal::BlockHeader) -> Self {
        BlockHeader {
            number: value.number,
            hash: value.hash,
            parent_hash: value.parent_hash,
            size: value.size,
            sha3_uncles: value.sha3_uncles,
            miner: value.miner,
            state_root: value.state_root,
            transactions_root: value.transactions_root,
            receipts_root: value.receipts_root,
            logs_bloom: value.logs_bloom,
            difficulty: value.difficulty,
            total_difficulty: value.total_difficulty,
            gas_limit: value.gas_limit,
            gas_used: value.gas_used,
            timestamp: to_u64(value.timestamp),
            extra_data: value.extra_data,
            mix_hash: value.mix_hash,
            nonce: value.nonce,
            base_fee_per_gas: value.base_fee_per_gas,
        }
    }
}

impl From<portal::Log> for Log {
    fn from(value: portal::Log) -> Self {
        Log {
            address: value.address,
            data: value.data,
            topics: value.topics,
            log_index: value.log_index,
            transaction_index: value.transaction_index,
        }
    }
}

impl From<portal::Transaction> for Transaction {
    fn from(value: portal::Transaction) -> Self {
        Transaction {
            transaction_index: value.transaction_index,
            hash: value.hash,
            nonce: value.nonce,
            from: value.from,
            to: value.to,
            input: value.input,
            value: value.value,
            gas: value.gas,
            gas_price: value.gas_price,
            max_fee_per_gas: value.max_fee_per_gas,
            max_priority_fee_per_gas: value.max_priority_fee_per_gas,
            v: value.v,
            r: value.r,
            s: value.s,
            y_parity: value.y_parity,
            gas_used: value.gas_used,
            cumulative_gas_used: value.cumulative_gas_used,
            effective_gas_price: value.effective_gas_price,
            r#type: value.r#type,
            status: value.status,
        }
    }
}

impl From<portal::TraceType> for TraceType {
    fn from(value: portal::TraceType) -> Self {
        match value {
            portal::TraceType::Call => TraceType::Call,
            portal::TraceType::Create => TraceType::Create,
            portal::TraceType::Reward => TraceType::Reward,
            portal::TraceType::Suicide => TraceType::Suicide,
        }
    }
}

impl From<portal::CallType> for CallType {
    fn from(value: portal::CallType) -> Self {
        match value {
            portal::CallType::Call => CallType::Call,
            portal::CallType::Callcode => CallType::Callcode,
            portal::CallType::Delegatecall => CallType::Delegatecall,
            portal::CallType::Staticcall => CallType::Staticcall,
        }
    }
}

impl From<portal::TraceAction> for TraceAction {
    fn from(value: portal::TraceAction) -> Self {
        TraceAction {
            from: value.from,
            to: value.to,
            value: value.value,
            gas: value.gas,
            input: value.input,
            r#type: value.r#type.and_then(|r#type| Some(CallType::from(r#type))),
        }
    }
}

impl From<portal::TraceResult> for TraceResult {
    fn from(value: portal::TraceResult) -> Self {
        TraceResult {
            gas_used: value.gas_used,
            address: value.address,
            output: value.output,
        }
    }
}

impl From<portal::Trace> for Trace {
    fn from(value: portal::Trace) -> Self {
        Trace {
            transaction_index: value.transaction_index,
            r#type: TraceType::from(value.r#type),
            error: value.error,
            revert_reason: value.revert_reason,
            action: value
                .action
                .and_then(|action| Some(TraceAction::from(action))),
            result: value
                .result
                .and_then(|result| Some(TraceResult::from(result))),
        }
    }
}

impl From<portal::Block> for Block {
    fn from(value: portal::Block) -> Self {
        Block {
            header: BlockHeader::from(value.header),
            logs: value
                .logs
                .unwrap_or_default()
                .into_iter()
                .map(|l| Log::from(l))
                .collect(),
            transactions: value
                .transactions
                .unwrap_or_default()
                .into_iter()
                .map(|t| Transaction::from(t))
                .collect(),
            traces: value
                .traces
                .unwrap_or_default()
                .into_iter()
                .map(|t| Trace::from(t))
                .collect(),
        }
    }
}
