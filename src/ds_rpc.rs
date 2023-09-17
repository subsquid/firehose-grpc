use crate::datasource::{
    Block, BlockHeader, BlockStream, DataRequest, DataSource, HashAndHeight, HotBlockStream,
    HotDataSource, HotSource, HotUpdate, Log, LogRequest, Trace, Transaction,
};
use anyhow::Context;
use async_stream::try_stream;
use ethers_core::types as evm;
use ethers_providers::{Http, Middleware, Provider};
use futures_core::Stream;
use futures_util::future::join_all;
use prefix_hex::ToHexPrefixed;
use std::cmp::min;
use std::collections::HashMap;
use std::time::Duration;

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

        let mut block = Block::try_from(block)?;
        block.logs = logs;
        block.transactions = transactions;
        block.traces = traces;
        blocks.push(block);
    }

    Ok(blocks)
}

async fn fetch_requested_data(
    client: &Provider<Http>,
    blocks: &mut [Block],
    request: &DataRequest,
) -> anyhow::Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    let range = (
        blocks.first().unwrap().header.number,
        blocks.last().unwrap().header.number,
    );

    let logs = get_logs(client, &range, &request.logs).await?;

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
    let mut tx_by_block: HashMap<u64, Vec<evm::Transaction>> = HashMap::new();
    for result in results {
        let transaction = result?.unwrap();
        let block_num = transaction
            .block_number
            .context("no block number")?
            .as_u64();
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
    let mut traces_by_block: HashMap<u64, Vec<Trace>> = HashMap::new();
    for result in results {
        let trace = result?;
        let _traces = traverse_trace(trace);
    }

    let mut logs_by_block: HashMap<u64, Vec<evm::Log>> = HashMap::new();
    for log in logs {
        let block_num = log.block_number.context("no block number")?.as_u64();
        if logs_by_block.contains_key(&block_num) {
            logs_by_block.get_mut(&block_num).unwrap().push(log);
        } else {
            logs_by_block.insert(block_num, vec![log]);
        }
    }

    for block in blocks {
        let mut logs = logs_by_block
            .remove(&block.header.number)
            .unwrap_or_default()
            .into_iter()
            .map(|log| Log::try_from(log))
            .collect::<Result<Vec<_>, _>>()?;
        logs.sort_by_key(|log| log.log_index);

        let mut transactions = tx_by_block
            .remove(&block.header.number)
            .unwrap_or_default()
            .into_iter()
            .map(|tx| {
                let receipt = receipt_by_hash.remove(&tx.hash).unwrap();
                Transaction::try_from((tx, receipt))
            })
            .collect::<Result<Vec<_>, _>>()?;
        transactions.sort_by_key(|tx| tx.transaction_index);

        let traces = traces_by_block
            .remove(&block.header.number)
            .unwrap_or_default();

        block.logs = logs;
        block.transactions = transactions;
        block.traces = traces;
    }

    Ok(())
}

impl TryFrom<evm::Block<evm::H256>> for Block {
    type Error = anyhow::Error;

    fn try_from(value: evm::Block<evm::H256>) -> Result<Self, Self::Error> {
        Ok(Block {
            header: BlockHeader {
                number: value.number.context("no number")?.as_u64(),
                hash: format!("{:?}", value.hash.context("no hash")?),
                parent_hash: format!("{:?}", value.parent_hash),
                size: value.size.context("no size")?.as_u64(),
                sha3_uncles: format!("{:?}", value.uncles_hash),
                miner: format!("{:?}", value.author.context("no author")?),
                state_root: format!("{:?}", value.state_root),
                transactions_root: format!("{:?}", value.transactions_root),
                receipts_root: format!("{:?}", value.receipts_root),
                logs_bloom: format!("{:?}", value.logs_bloom.context("no logs bloom")?),
                difficulty: format!("{:#x}", value.difficulty),
                total_difficulty: format!(
                    "{:#x}",
                    value.total_difficulty.context("no total difficulty")?
                ),
                gas_limit: format!("{:#x}", value.gas_limit),
                gas_used: format!("{:#x}", value.gas_used),
                timestamp: value.timestamp.as_u64(),
                extra_data: value.extra_data.to_hex_prefixed(),
                mix_hash: format!("{:?}", value.mix_hash.context("no mix hash")?),
                nonce: format!("{:?}", value.nonce.context("no nonce")?),
                base_fee_per_gas: value
                    .base_fee_per_gas
                    .and_then(|val| Some(format!("{:#x}", val))),
            },
            logs: vec![],
            traces: vec![],
            transactions: vec![],
        })
    }
}

impl TryFrom<evm::Log> for Log {
    type Error = anyhow::Error;

    fn try_from(value: evm::Log) -> Result<Self, Self::Error> {
        Ok(Log {
            address: format!("{:?}", value.address),
            data: value.data.to_hex_prefixed(),
            topics: value
                .topics
                .into_iter()
                .map(|topic| format!("{:?}", topic))
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
            hash: format!("{:?}", tx.hash),
            from: format!("{:?}", tx.from),
            to: tx.to.and_then(|val| Some(format!("{:?}", val))),
            transaction_index: tx
                .transaction_index
                .context("no transaction index")?
                .as_u32(),
            input: tx.input.to_hex_prefixed(),
            r#type: i32::try_from(tx.transaction_type.context("no transaction type")?)
                .map_err(anyhow::Error::msg)?,
            nonce: tx.nonce.as_u64(),
            r: format!("{:#x}", tx.r),
            s: format!("{:#x}", tx.s),
            v: format!("{:#x}", tx.v),
            value: format!("{:#x}", tx.value),
            gas: format!("{:#x}", tx.gas),
            gas_price: format!("{:#x}", tx.gas_price.context("no gas price")?),
            max_fee_per_gas: tx
                .max_fee_per_gas
                .and_then(|val| Some(format!("{:#x}", val))),
            max_priority_fee_per_gas: tx
                .max_priority_fee_per_gas
                .and_then(|val| Some(format!("{:#x}", val))),
            y_parity: None,
            cumulative_gas_used: format!("{:#x}", receipt.cumulative_gas_used),
            effective_gas_price: format!(
                "{:#x}",
                receipt
                    .effective_gas_price
                    .context("no effective gas price")?
            ),
            gas_used: format!("{:#x}", receipt.gas_used.context("no gas used")?),
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

    async fn get_block_hash(&self, height: u64) -> anyhow::Result<String> {
        let block = self
            .client
            .get_block(height)
            .await?
            .context(format!("block №{} not found", height))?;
        let hash = format!("{:?}", block.hash.context("hash is empty")?);
        Ok(hash)
    }
}

#[async_trait::async_trait]
impl HotSource for RpcDataSource {
    fn get_hot_blocks(
        &self,
        request: DataRequest,
        state: HashAndHeight,
    ) -> anyhow::Result<HotBlockStream> {
        let client = self.client.clone();
        let finality_confirmation = self.finality_confirmation;

        Ok(Box::new(try_stream! {
            let mut nav = ForkNavigator::new(client.clone(), state);

            for await result in get_height_updates(client.clone(), request.from) {
                let top = result?;
                let finalized = top.saturating_sub(finality_confirmation);
                let height = nav.get_height();

                for number in height + 1..top {
                    let mut update = nav.r#move(number, min(number, finalized)).await?;
                    fetch_requested_data(&client, &mut update.blocks, &request).await?;
                    let finalized_head = update.finalized_head.height;

                    yield update;

                    if let Some(to) = request.to {
                        if finalized_head >= to {
                            return
                        }
                    }
                }
            }
        }))
    }

    fn as_ds(&self) -> &(dyn DataSource + Send + Sync) {
        self
    }
}

impl HotDataSource for RpcDataSource {}

impl RpcDataSource {
    pub fn new(url: String, finality_confirmation: u64) -> RpcDataSource {
        let client = Provider::<Http>::try_from(url).unwrap();
        RpcDataSource {
            client,
            finality_confirmation,
        }
    }
}

fn get_height_updates(
    client: Provider<Http>,
    from: u64,
) -> impl Stream<Item = anyhow::Result<u64>> {
    try_stream! {
        let mut height = from;
        let mut current = client.get_block_number().await?.as_u64();
        let interval = Duration::from_secs(1);
        loop {
            while current < height {
                tokio::time::sleep(interval).await;
                current = client.get_block_number().await?.as_u64();
            }
            yield height;
            height += 1;
        }
    }
}

struct ForkNavigator {
    chain: Vec<HashAndHeight>,
    client: Provider<Http>,
}

impl ForkNavigator {
    pub fn new(client: Provider<Http>, state: HashAndHeight) -> ForkNavigator {
        ForkNavigator {
            chain: vec![state],
            client,
        }
    }

    pub fn get_height(&self) -> u64 {
        self.chain
            .last()
            .expect("chain state can't be empty")
            .height
    }

    pub async fn r#move(&mut self, best: u64, finalized: u64) -> anyhow::Result<HotUpdate> {
        let mut chain = self.chain.clone();
        let mut new_blocks = vec![];

        let best_head = if best > chain.last().unwrap().height {
            let new_block: Block = self.client.get_block(best).await?.unwrap().try_into()?;
            let best_head = HashAndHeight {
                hash: new_block.header.parent_hash.clone(),
                height: new_block.header.number - 1,
            };
            new_blocks.push(new_block);
            Some(best_head)
        } else {
            None
        };

        if let Some(mut best_head) = best_head {
            while chain.last().unwrap().height < best_head.height {
                let block: Block = self
                    .client
                    .get_block(evm::H256::from_slice(best_head.hash.as_bytes()))
                    .await?
                    .expect("consistency error")
                    .try_into()?;
                best_head = HashAndHeight {
                    hash: block.header.parent_hash.clone(),
                    height: block.header.number - 1,
                };
                new_blocks.push(block);
            }

            while chain.last().unwrap().hash != best_head.hash {
                let block: Block = self
                    .client
                    .get_block(evm::H256::from_slice(best_head.hash.as_bytes()))
                    .await?
                    .expect("consistency error")
                    .try_into()?;
                best_head = HashAndHeight {
                    hash: block.header.parent_hash.clone(),
                    height: block.header.number - 1,
                };
                new_blocks.push(block);
                chain.pop();
            }
        }

        new_blocks.reverse();
        for block in &new_blocks {
            chain.push(HashAndHeight {
                hash: block.header.hash.clone(),
                height: block.header.number,
            });
        }

        let finalized_head = if finalized > chain[0].height {
            let finalized_pos = usize::try_from(finalized - chain[0].height)?;
            Some(chain[finalized_pos].clone())
        } else {
            None
        };

        if let Some(finalized_head) = finalized_head {
            if finalized_head.height >= chain[0].height {
                let finalized_pos = usize::try_from(finalized_head.height - chain[0].height)?;
                chain = chain[finalized_pos..].to_vec();
            }
        }

        self.chain = chain;

        let base_head = if new_blocks.is_empty() {
            self.chain.last().unwrap().clone()
        } else {
            HashAndHeight {
                height: new_blocks[0].header.number - 1,
                hash: new_blocks[0].header.parent_hash.clone(),
            }
        };

        Ok(HotUpdate {
            blocks: new_blocks,
            base_head,
            finalized_head: self.chain[0].clone(),
        })
    }
}
