use crate::datasource::{
    Block, BlockHeader, BlockStream, CallType, DataRequest, DataSource, Log, Trace, TraceAction,
    TraceResult, TraceType, Transaction,
};
use crate::{
    archive,
    archive::{
        Archive, BatchRequest, BlockFieldSelection, FieldSelection, LogFieldSelection, LogRequest,
        TraceFieldSelection, TxFieldSelection, TxRequest,
    },
};
use async_stream::try_stream;
use serde_json::Number;
use std::sync::Arc;

#[derive(Debug)]
pub struct ArchiveDataSource {
    archive: Arc<Archive>,
}

impl ArchiveDataSource {
    pub fn new(archive: Arc<Archive>) -> ArchiveDataSource {
        ArchiveDataSource { archive }
    }
}

#[async_trait::async_trait]
impl DataSource for ArchiveDataSource {
    fn get_finalized_blocks(&self, request: DataRequest) -> anyhow::Result<BlockStream> {
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
                transaction_index: true,
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
                    transaction: true,
                    transaction_traces: true,
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
            fields.trace = Some(TraceFieldSelection {
                transaction_index: true,
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
            let transactions = request
                .transactions
                .into_iter()
                .map(|r| TxRequest {
                    to: r.address,
                    sighash: r.sighash,
                    traces: true,
                })
                .collect();
            Some(transactions)
        };

        let mut req = BatchRequest {
            from_block: request.from,
            to_block: request.to,
            fields: Some(fields),
            logs,
            transactions,
        };

        let archive = self.archive.clone();
        Ok(Box::new(try_stream! {
            loop {
                let height = archive.height().await?;
                if req.from_block > height {
                    break;
                }

                let blocks = archive.query(&req).await?;
                let last_block_num = blocks[blocks.len() - 1].header.number;
                let blocks = blocks.into_iter().map(|b| Block::from(b)).collect();

                yield blocks;

                if let Some(to_block) = req.to_block {
                    if last_block_num == to_block {
                        break;
                    }
                }

                req.from_block = last_block_num + 1;
            }
        }))
    }

    async fn get_finalized_height(&self) -> anyhow::Result<u64> {
        self.archive.height().await
    }

    async fn get_block_hash(&self, _height: u64) -> anyhow::Result<String> {
        todo!()
    }
}

fn number_to_u64(value: Number) -> u64 {
    if let Some(val) = value.as_u64() {
        return val;
    }
    if let Some(val) = value.as_f64() {
        return val as u64;
    }
    unimplemented!()
}

impl From<archive::BlockHeader> for BlockHeader {
    fn from(value: archive::BlockHeader) -> Self {
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
            timestamp: number_to_u64(value.timestamp),
            extra_data: value.extra_data,
            mix_hash: value.mix_hash,
            nonce: value.nonce,
            base_fee_per_gas: value.base_fee_per_gas,
        }
    }
}

impl From<archive::Log> for Log {
    fn from(value: archive::Log) -> Self {
        Log {
            address: value.address,
            data: value.data,
            topics: value.topics,
            log_index: value.log_index,
            transaction_index: value.transaction_index,
        }
    }
}

impl From<archive::Transaction> for Transaction {
    fn from(value: archive::Transaction) -> Self {
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

impl From<archive::TraceType> for TraceType {
    fn from(value: archive::TraceType) -> Self {
        match value {
            archive::TraceType::Call => TraceType::Call,
            archive::TraceType::Create => TraceType::Create,
            archive::TraceType::Reward => TraceType::Reward,
            archive::TraceType::Suicide => TraceType::Suicide,
        }
    }
}

impl From<archive::CallType> for CallType {
    fn from(value: archive::CallType) -> Self {
        match value {
            archive::CallType::Call => CallType::Call,
            archive::CallType::Callcode => CallType::Callcode,
            archive::CallType::Delegatecall => CallType::Delegatecall,
            archive::CallType::Staticcall => CallType::Staticcall,
        }
    }
}

impl From<archive::TraceAction> for TraceAction {
    fn from(value: archive::TraceAction) -> Self {
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

impl From<archive::TraceResult> for TraceResult {
    fn from(value: archive::TraceResult) -> Self {
        TraceResult {
            gas_used: value.gas_used,
            address: value.address,
            output: value.output,
        }
    }
}

impl From<archive::Trace> for Trace {
    fn from(value: archive::Trace) -> Self {
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

impl From<archive::Block> for Block {
    fn from(value: archive::Block) -> Self {
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
