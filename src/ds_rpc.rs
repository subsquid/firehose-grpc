use crate::datasource::{
    Block, BlockHeader, BlockStream, DataRequest, DataSource, Log, LogRequest, Trace, Transaction,
};
use anyhow::Context;
use async_stream::try_stream;
use ethers_core::types as evm;
use ethers_providers::{Http, Middleware, Provider};
use futures_util::future::join_all;
use prefix_hex::ToHexPrefixed;
use std::cmp::min;
use std::collections::HashMap;

type Range = (u64, u64);

async fn get_finalized_height(
    client: &Provider<Http>,
    finality_confirmation: u64,
) -> anyhow::Result<u64> {
    let height = client.get_block_number().await?.as_u64();
    Ok(height.saturating_sub(finality_confirmation))
}

async fn get_logs(
    client: &Provider<Http>,
    range: &Range,
    requests: &Vec<LogRequest>,
) -> anyhow::Result<Vec<evm::Log>> {
    let mut logs = vec![];

    for request in requests {
        let mut filter = evm::Filter::new().from_block(range.0).to_block(range.1);

        if !request.address.is_empty() {
            let address: Vec<_> = request
                .address
                .iter()
                .map(|address| evm::H160::from_slice(address.as_bytes()))
                .collect();
            filter = filter.address(address);
        }

        if !request.topic0.is_empty() {
            let topic: Vec<_> = request
                .topic0
                .iter()
                .map(|topic| evm::H256::from_slice(topic.as_bytes()))
                .collect();
            filter = filter.topic0(topic);
        }

        let mut request_logs = client.get_logs(&filter).await?;
        logs.append(&mut request_logs);
    }

    Ok(logs)
}

fn traverse_trace(trace: evm::GethTrace) -> Vec<Trace> {
    let traces = vec![];
    traces
}

async fn get_stride(
    client: &Provider<Http>,
    range: &Range,
    request: &DataRequest,
) -> anyhow::Result<Vec<Block>> {
    let logs = get_logs(client, range, &request.logs).await?;

    let mut block_numbers: Vec<_> = logs
        .iter()
        .map(|log| log.block_number.unwrap().clone())
        .collect();
    block_numbers.sort();
    block_numbers.dedup();

    let mut tx_hashes: Vec<_> = logs
        .iter()
        .map(|log| log.transaction_hash.unwrap().clone())
        .collect();
    tx_hashes.sort();
    tx_hashes.dedup();

    let futures: Vec<_> = tx_hashes
        .iter()
        .map(|hash| client.get_transaction(*hash))
        .collect();
    let results = join_all(futures).await;
    let mut tx_by_block: HashMap<evm::U64, Vec<evm::Transaction>> = HashMap::new();
    for result in results {
        let transaction = result?.unwrap();
        let block_num = transaction.block_number.context("no block number")?;
        if tx_by_block.contains_key(&block_num) {
            tx_by_block.get_mut(&block_num).unwrap().push(transaction);
        } else {
            tx_by_block.insert(block_num, vec![transaction]);
        }
    }

    let futures: Vec<_> = tx_hashes
        .iter()
        .map(|hash| client.get_transaction_receipt(*hash))
        .collect();
    let results = join_all(futures).await;
    let mut receipt_by_hash: HashMap<evm::H256, evm::TransactionReceipt> = HashMap::new();
    for result in results {
        let receipt = result?.unwrap();
        receipt_by_hash.insert(receipt.transaction_hash, receipt);
    }

    let futures: Vec<_> = tx_hashes
        .iter()
        .map(|hash| {
            let options = evm::GethDebugTracingOptions {
                tracer: Some(evm::GethDebugTracerType::BuiltInTracer(
                    evm::GethDebugBuiltInTracerType::CallTracer,
                )),
                tracer_config: Some(evm::GethDebugTracerConfig::BuiltInTracer(
                    evm::GethDebugBuiltInTracerConfig::CallTracer(evm::CallConfig {
                        only_top_call: Some(false),
                        with_log: Some(true),
                    }),
                )),
                ..Default::default()
            };
            client.debug_trace_transaction(*hash, options)
        })
        .collect();
    let results = join_all(futures).await;
    let mut traces_by_block: HashMap<evm::U64, Vec<Trace>> = HashMap::new();
    for result in results {
        let trace = result?;
        let _traces = traverse_trace(trace);
    }

    let futures: Vec<_> = block_numbers
        .iter()
        .map(|num| client.get_block(*num))
        .collect();
    let results = join_all(futures).await;

    let mut logs_by_block: HashMap<evm::U64, Vec<evm::Log>> = HashMap::new();
    for log in logs {
        let block_num = log.block_number.context("no block number")?;
        if logs_by_block.contains_key(&block_num) {
            logs_by_block.get_mut(&block_num).unwrap().push(log);
        } else {
            logs_by_block.insert(block_num, vec![log]);
        }
    }

    let mut blocks = Vec::with_capacity(block_numbers.len());
    for result in results {
        let block = result?.unwrap();
        let block_num = block.number.context("no number")?;

        let mut logs = logs_by_block
            .remove(&block_num)
            .unwrap_or_default()
            .into_iter()
            .map(|log| Log::try_from(log))
            .collect::<Result<Vec<_>, _>>()?;
        logs.sort_by_key(|log| log.log_index);

        let mut transactions = tx_by_block
            .remove(&block_num)
            .unwrap_or_default()
            .into_iter()
            .map(|tx| {
                let receipt = receipt_by_hash.remove(&tx.hash).unwrap();
                Transaction::try_from((tx, receipt))
            })
            .collect::<Result<Vec<_>, _>>()?;
        transactions.sort_by_key(|tx| tx.transaction_index);

        let traces = traces_by_block.remove(&block_num).unwrap_or_default();

        blocks.push(Block {
            header: BlockHeader::try_from(block)?,
            logs,
            transactions,
            traces,
        });
    }

    Ok(vec![])
}

impl TryFrom<evm::Block<evm::H256>> for BlockHeader {
    type Error = anyhow::Error;

    fn try_from(value: evm::Block<evm::H256>) -> Result<Self, Self::Error> {
        Ok(BlockHeader {
            number: value.number.context("no number")?.as_u64(),
            hash: value.hash.context("no hash")?.to_string(),
            parent_hash: value.parent_hash.to_string(),
            size: value.size.context("no size")?.as_u64(),
            sha3_uncles: value.uncles_hash.to_string(),
            miner: value.author.context("no author")?.to_string(),
            state_root: value.state_root.to_string(),
            transactions_root: value.transactions_root.to_string(),
            receipts_root: value.receipts_root.to_string(),
            logs_bloom: value.logs_bloom.context("no logs bloom")?.to_string(),
            difficulty: value.difficulty.to_string(),
            total_difficulty: value
                .total_difficulty
                .context("no total difficulty")?
                .to_string(),
            gas_limit: value.gas_limit.to_string(),
            gas_used: value.gas_used.to_string(),
            timestamp: value.timestamp.as_u64(),
            extra_data: value.extra_data.to_hex_prefixed(),
            mix_hash: value.mix_hash.context("no mix hash")?.to_string(),
            nonce: value.nonce.context("no nonce")?.to_string(),
            base_fee_per_gas: value.base_fee_per_gas.and_then(|val| Some(val.to_string())),
        })
    }
}

impl TryFrom<evm::Log> for Log {
    type Error = anyhow::Error;

    fn try_from(value: evm::Log) -> Result<Self, Self::Error> {
        Ok(Log {
            address: value.address.to_string(),
            data: value.data.to_hex_prefixed(),
            topics: value
                .topics
                .into_iter()
                .map(|topic| topic.to_string())
                .collect(),
            log_index: value.log_index.context("no log index")?.as_u32(),
            transaction_index: value
                .transaction_index
                .context("no transaction index")?
                .as_u32(),
        })
    }
}

impl TryFrom<(evm::Transaction, evm::TransactionReceipt)> for Transaction {
    type Error = anyhow::Error;

    fn try_from(value: (evm::Transaction, evm::TransactionReceipt)) -> Result<Self, Self::Error> {
        let tx = value.0;
        let receipt = value.1;
        Ok(Transaction {
            hash: tx.hash.to_string(),
            from: tx.from.to_string(),
            to: tx.to.and_then(|val| Some(val.to_string())),
            transaction_index: tx
                .transaction_index
                .context("no transaction index")?
                .as_u32(),
            input: tx.input.to_hex_prefixed(),
            r#type: i32::try_from(tx.transaction_type.context("no transaction type")?)
                .map_err(anyhow::Error::msg)?,
            nonce: tx.nonce.as_u64(),
            r: tx.r.to_string(),
            s: tx.s.to_string(),
            v: tx.v.to_string(),
            value: tx.value.to_string(),
            gas: tx.gas.to_string(),
            gas_price: tx.gas_price.context("no gas price")?.to_string(),
            max_fee_per_gas: tx.max_fee_per_gas.and_then(|val| Some(val.to_string())),
            max_priority_fee_per_gas: tx
                .max_priority_fee_per_gas
                .and_then(|val| Some(val.to_string())),
            y_parity: None,
            cumulative_gas_used: receipt.cumulative_gas_used.to_string(),
            effective_gas_price: receipt
                .effective_gas_price
                .context("no effective gas price")?
                .to_string(),
            gas_used: receipt.gas_used.context("no gas used")?.to_string(),
            status: i32::try_from(receipt.status.context("no status")?)
                .map_err(anyhow::Error::msg)?,
        })
    }
}

fn split_range(from: u64, to: u64) -> Vec<Range> {
    assert!(from <= to);
    let step = 100;
    let mut from = from;
    let mut ranges = vec![];
    while from <= to {
        let to_ = min(from + step - 1, to);
        ranges.push((from, to_));
        from = to_ + 1;
    }
    ranges
}

pub struct RpcDataSource {
    client: Provider<Http>,
    finality_confirmation: u64,
}

#[async_trait::async_trait]
impl DataSource for RpcDataSource {
    fn get_finalized_blocks(&self, request: DataRequest) -> anyhow::Result<BlockStream> {
        let client = self.client.clone();
        let finality_confirmation = self.finality_confirmation;

        Ok(Box::new(try_stream! {
            let height = get_finalized_height(&client, finality_confirmation).await?;
            let to = if let Some(to) = request.to {
                min(height, to)
            } else {
                height
            };

            if request.from > to {
                return
            }

            let ranges = split_range(request.from, to);
            for chunk in ranges.chunks(5) {
                let futures: Vec<_> = chunk.into_iter().map(|range| get_stride(&client, range, &request)).collect();
                let results = join_all(futures).await;

                let mut blocks = vec![];
                for result in results {
                    let mut stride_blocks = result?;
                    blocks.append(&mut stride_blocks);
                }
                yield blocks;
            }
        }))
    }

    async fn get_finalized_height(&self) -> anyhow::Result<u64> {
        let height = get_finalized_height(&self.client, self.finality_confirmation).await?;
        Ok(height)
    }
}

impl RpcDataSource {
    pub fn new(url: String, finality_confirmation: u64) -> RpcDataSource {
        let client = Provider::<Http>::try_from(url).unwrap();
        RpcDataSource {
            client,
            finality_confirmation,
        }
    }
}
