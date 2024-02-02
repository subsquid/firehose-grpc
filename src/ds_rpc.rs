use crate::datasource::{
    Block, BlockHeader, BlockStream, CallType, DataRequest, DataSource, HashAndHeight,
    HotBlockStream, HotDataSource, HotSource, HotUpdate, Log, LogRequest, Trace, TraceAction,
    TraceResult, TraceType, Transaction, TraceRequest,
};
use anyhow::Context;
use async_stream::try_stream;
use ethers_core::types as evm;
use ethers_providers::{Http, Middleware, Provider};
use futures_core::Stream;
use futures_util::future::join_all;
use prefix_hex::ToHexPrefixed;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

type Range = (u64, u64);

async fn get_finalized_height(
    height_tracker: &HeightTracker,
    finality_confirmation: u64,
) -> anyhow::Result<u64> {
    let height = height_tracker.height().await?;
    Ok(height.saturating_sub(finality_confirmation))
}

async fn get_logs(
    client: &Provider<Http>,
    range: &Range,
    requests: &Vec<LogRequest>,
) -> anyhow::Result<Vec<evm::Log>> {
    let mut logs = vec![];

    for request in requests {
        let mut filter = evm::Filter::new()
            .from_block(range.0)
            .to_block(range.1);

        if !request.address.is_empty() {
            let address = request
                .address
                .iter()
                .map(|address| address.parse())
                .collect::<Result<Vec<_>, _>>()?;
            filter = filter.address(address);
        }

        if !request.topic0.is_empty() {
            let topic = request
                .topic0
                .iter()
                .map(|topic| topic.parse::<evm::H256>())
                .collect::<Result<Vec<_>, _>>()?;
            filter = filter.topic0(topic);
        }

        let mut request_logs = client.get_logs(&filter).await?;
        logs.append(&mut request_logs);
    }

    Ok(logs)
}

async fn get_traces(
    client: &Provider<Http>,
    range: &Range,
    requests: &Vec<TraceRequest>
) -> anyhow::Result<Vec<evm::Trace>> {
    if requests.is_empty() {
        return Ok(vec![])
    }

    let mut address = requests.iter()
        .flat_map(|request| request.address.clone())
        .map(|address| address.parse::<evm::H160>())
        .collect::<Result<Vec<_>, _>>()?;
    address.sort();
    address.dedup();

    let filter = evm::TraceFilter::default()
        .from_block(range.0)
        .to_block(range.1)
        .to_address(address);

    let mut traces: Vec<_> = client.trace_filter(filter).await?
        .into_iter()
        .filter(|trace| {
            if let evm::Action::Call(action) = &trace.action {
                let to = format!("{:?}", action.to);
                let input = action.input.to_hex_prefixed();
                let sighash = to_sighash(&input);
                if let Some(sighash) = sighash {
                    for request in requests {
                        if !request.address.contains(&to) {
                            continue;
                        }
                        if request.sighash.contains(&sighash.to_string()) {
                            return true;
                        }
                    }
                }
            }
            false
        })
        .collect();

    traces.sort_by_key(|t| (t.transaction_hash.unwrap(), t.trace_address.clone()));

    Ok(traces)
}

fn to_sighash(input: &str) -> Option<&str> {
    if input.len() >= 10 {
        Some(&input[..10])
    } else {
        None
    }
}

async fn get_stride(
    client: &Provider<Http>,
    range: &Range,
    request: &DataRequest,
) -> anyhow::Result<Vec<Block>> {
    let rpc_blocks = get_blocks(client, range).await?;
    let blocks = get_requested_data(client, rpc_blocks, request).await?;
    Ok(blocks)
}

async fn get_blocks(
    client: &Provider<Http>,
    range: &Range,
) -> anyhow::Result<Vec<evm::Block<evm::Transaction>>> {
    let futures: Vec<_> = (range.0..=range.1)
        .map(|num| client.get_block_with_txs(num))
        .collect();
    join_all(futures)
        .await
        .into_iter()
        .map(|res| Ok(res?.expect("unfinalized block was requested")))
        .collect()
}

async fn get_requested_data(
    client: &Provider<Http>,
    mut blocks: Vec<evm::Block<evm::Transaction>>,
    request: &DataRequest,
) -> anyhow::Result<Vec<Block>> {
    if blocks.is_empty() {
        return Ok(vec![]);
    }

    let range = (
        blocks.first().unwrap().number.unwrap().as_u64(),
        blocks.last().unwrap().number.unwrap().as_u64(),
    );

    let logs = get_logs(client, &range, &request.logs).await?;
    let traces = get_traces(client, &range, &request.traces).await?;

    let mut tx_hashes = HashSet::new();
    let mut has_root_trace: HashMap<evm::H256, bool> = HashMap::new();

    for log in logs {
        let tx_hash = log.transaction_hash.unwrap().clone();
        tx_hashes.insert(tx_hash);
    }

    let mut traces_by_block: HashMap<u64, Vec<evm::Trace>> = HashMap::new();
    for trace in traces {
        let tx_hash = trace.transaction_hash.unwrap().clone();
        tx_hashes.insert(tx_hash);

        if trace.trace_address.is_empty() {
            has_root_trace.insert(tx_hash.clone(), true);
        }

        if has_root_trace.contains_key(&tx_hash) {
            if traces_by_block.contains_key(&trace.block_number) {
                traces_by_block.get_mut(&trace.block_number).unwrap().push(trace);
            } else {
                traces_by_block.insert(trace.block_number, vec![trace]);
            }
        }
    }

    let mut tx_by_block = HashMap::new();
    for block in &mut blocks {
        let block_num = block.number.unwrap().as_u64();
        let mut transactions = vec![];

        for tx in block.transactions.drain(..) {
            if tx_hashes.contains(&tx.hash) {
                transactions.push(tx);
            }
        }

        tx_by_block.insert(block_num, transactions);
    }

    let futures: Vec<_> = tx_hashes
        .iter()
        .map(|hash| client.get_transaction_receipt(*hash))
        .collect();
    let results = join_all(futures).await;
    let mut logs_by_block: HashMap<u64, Vec<evm::Log>> = HashMap::new();
    let mut receipt_by_hash: HashMap<evm::H256, evm::TransactionReceipt> = HashMap::new();
    for result in results {
        let mut receipt = result?.unwrap();
        let block_num = receipt.block_number.unwrap().as_u64();

        if logs_by_block.contains_key(&block_num) {
            logs_by_block.get_mut(&block_num).unwrap().append(&mut receipt.logs);
        } else {
            let logs = receipt.logs.drain(..).collect();
            logs_by_block.insert(block_num, logs);
        }

        receipt_by_hash.insert(receipt.transaction_hash, receipt);
    }

    let futures: Vec<_> = tx_hashes
        .iter()
        .filter_map(|hash| {
            if !has_root_trace.contains_key(hash) {
                Some(client.trace_transaction(*hash))
            } else {
                None
            }
        })
        .collect();
    let results = join_all(futures).await;
    for result in results {
        let mut traces = result?;
        let call = &traces[0];
        if traces_by_block.contains_key(&call.block_number) {
            traces_by_block.get_mut(&call.block_number).unwrap().append(&mut traces);
        } else {
            traces_by_block.insert(call.block_number, traces);
        }
    }

    let blocks = blocks
        .into_iter()
        .map(|block| {
            let mut block = Block::try_from(block)?;

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
                .unwrap_or_default()
                .into_iter()
                .map(|trace| Trace::try_from(trace))
                .collect::<Result<Vec<_>, _>>()?;

            block.logs = logs;
            block.transactions = transactions;
            block.traces = traces;

            Ok(block)
        })
        .collect::<Result<Vec<Block>, anyhow::Error>>()?;

    Ok(blocks)
}

impl TryFrom<&String> for TraceType {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "CALL" | "CALLCODE" | "STATICCALL" | "DELEGATECALL" => Ok(TraceType::Call),
            "CREATE" | "CREATE2" => Ok(TraceType::Create),
            "SELFDESTRUCT" => Ok(TraceType::Suicide),
            _ => anyhow::bail!("unknown frame type - {}", value),
        }
    }
}

impl TryFrom<&String> for CallType {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "CALL" => Ok(CallType::Call),
            "CALLCODE" => Ok(CallType::Callcode),
            "STATICCALL" => Ok(CallType::Staticcall),
            "DELEGATECALL" => Ok(CallType::Delegatecall),
            _ => anyhow::bail!("unknown call type - {}", value),
        }
    }
}

impl TryFrom<evm::CallFrame> for Trace {
    type Error = anyhow::Error;

    fn try_from(value: evm::CallFrame) -> Result<Self, Self::Error> {
        let r#type = TraceType::try_from(&value.typ)?;
        let action = match r#type {
            TraceType::Call => {
                let to = match value.to.as_ref().context("no to")? {
                    evm::NameOrAddress::Name(_) => anyhow::bail!("names aren't supported"),
                    evm::NameOrAddress::Address(address) => address,
                };
                Some(TraceAction {
                    from: Some(format!("{:?}", value.from)),
                    gas: Some(format!("{:#x}", value.gas)),
                    input: Some(value.input.to_hex_prefixed()),
                    to: Some(format!("{:?}", to)),
                    r#type: Some(CallType::try_from(&value.typ)?),
                    value: value.value.and_then(|val| Some(format!("{:#x}", val))),
                })
            }
            TraceType::Create => Some(TraceAction {
                from: Some(format!("{:?}", value.from)),
                gas: Some(format!("{:#x}", value.gas)),
                input: Some(value.input.to_hex_prefixed()),
                to: None,
                r#type: None,
                value: value.value.and_then(|val| Some(format!("{:#x}", val))),
            }),
            TraceType::Suicide => None,
            TraceType::Reward => unreachable!(),
        };
        let result = match r#type {
            TraceType::Call => Some(TraceResult {
                address: None,
                gas_used: Some(format!("{:#x}", value.gas_used)),
                output: value.output.and_then(|val| Some(val.to_hex_prefixed())),
            }),
            TraceType::Create => Some(TraceResult {
                address: value.to.and_then(|val| Some(format!("{:?}", val))),
                gas_used: Some(format!("{:#x}", value.gas_used)),
                output: value.output.and_then(|val| Some(val.to_hex_prefixed())),
            }),
            TraceType::Suicide => None,
            TraceType::Reward => unreachable!(),
        };

        Ok(Trace {
            transaction_index: 0, // call_frame has no info about its tx
            r#type,
            action,
            result,
            error: value.error,
            revert_reason: None, // revert_reason isn't presented in ethers-core crate
        })
    }
}

impl TryFrom<evm::Block<evm::Transaction>> for Block {
    type Error = anyhow::Error;

    fn try_from(value: evm::Block<evm::Transaction>) -> Result<Self, Self::Error> {
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

impl TryFrom<evm::Trace> for Trace {
    type Error = anyhow::Error;

    fn try_from(value: evm::Trace) -> Result<Self, Self::Error> {
        match &value.action {
            evm::Action::Call(action) => {
                Ok(Trace {
                    transaction_index: value.transaction_position.context("no transaction position")?.try_into()?,
                    r#type: TraceType::Call,
                    error: value.error,
                    revert_reason: None,
                    action: Some(TraceAction {
                        from: Some(format!("{:?}", action.from)),
                        to: Some(format!("{:?}", action.to)),
                        gas: Some(format!("{:#x}", action.gas)),
                        input: Some(action.input.to_hex_prefixed()),
                        r#type: match action.call_type {
                            evm::CallType::None => None,
                            evm::CallType::CallCode => Some(CallType::Callcode),
                            evm::CallType::DelegateCall => Some(CallType::Delegatecall),
                            evm::CallType::StaticCall => Some(CallType::Staticcall),
                            evm::CallType::Call => Some(CallType::Call),
    
                        },
                        value: Some(format!("{:#x}", action.value)),
                    }),
                    result: value.result.and_then(|result| {
                        if let evm::Res::Call(res) = &result {
                            Some(TraceResult {
                                gas_used: Some(format!("{:#x}", res.gas_used)),
                                address: None,
                                output: Some(res.output.to_hex_prefixed()),
                            })
                        } else {
                            None
                        }
                    }),
                })
            }
            evm::Action::Create(action) => {
                Ok(Trace {
                    transaction_index: value.transaction_position.context("no transaction position")?.try_into()?,
                    r#type: TraceType::Call,
                    error: value.error,
                    revert_reason: None,
                    action: Some(TraceAction {
                        from: Some(format!("{:?}", action.from)),
                        value: Some(format!("{:#x}", action.value)),
                        gas: Some(format!("{:#x}", action.gas)),
                        to: None,
                        input: None,
                        r#type: None,
                    }),
                    result: value.result.and_then(|result| {
                        if let evm::Res::Create(res) = &result {
                            Some(TraceResult {
                                gas_used: Some(format!("{:#x}", res.gas_used)),
                                address: Some(format!("{:?}", res.address)),
                                output: None,
                            })
                        } else {
                            None
                        }
                    }),
                })
            },
            _ => anyhow::bail!("only call and create traces are supported")
        }
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
    height_tracker: Arc<HeightTracker>,
    finality_confirmation: u64,
}

#[async_trait::async_trait]
impl DataSource for RpcDataSource {
    fn get_finalized_blocks(
        &self,
        request: DataRequest,
        _stop_on_head: bool,
    ) -> anyhow::Result<BlockStream> {
        let client = self.client.clone();
        let finality_confirmation = self.finality_confirmation;
        let height_tracker = self.height_tracker.clone();

        Ok(Box::new(try_stream! {
            let height = get_finalized_height(&height_tracker, finality_confirmation).await?;
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
        let height = get_finalized_height(&self.height_tracker, self.finality_confirmation).await?;
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
        let height_tracker = self.height_tracker.clone();
        Ok(Box::new(try_stream! {
            let mut nav = ForkNavigator::new(state, |block_id| {
                let client = client.clone();
                let request = request.clone();
                async move {
                    let rpc_block = client.get_block_with_txs(block_id).await?
                        .ok_or(anyhow::anyhow!("consistency error"))?;
                    let mut blocks = get_requested_data(&client, vec![rpc_block], &request).await?;
                    let block = blocks.remove(0);
                    Ok(block)
                }
            });

            for await result in get_height_updates(height_tracker, request.from) {
                let top = result?;
                let finalized = top.saturating_sub(finality_confirmation);
                let height = nav.get_height();

                for number in height + 1..top {
                    let mut retries = 0;
                    let update = loop {
                        match nav.r#move(number, min(number, finalized)).await {
                            Ok(update) => break update,
                            Err(err) => {
                                match err.downcast::<&str>() {
                                    Ok(err_msg) => {
                                        if err_msg == "consistency error" && retries < 10 {
                                            retries += 1;
                                            let duration = Duration::from_millis(200 * retries);
                                            tokio::time::sleep(duration).await;
                                            continue;
                                        }
                                    },
                                    Err(err) => Err(err)?,
                                };
                            }
                        }
                    };
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
        let height_tracker = Arc::new(HeightTracker::new(client.clone(), Duration::from_secs(1)));
        RpcDataSource {
            client,
            height_tracker,
            finality_confirmation,
        }
    }
}

fn get_height_updates(
    height_tracker: Arc<HeightTracker>,
    from: u64,
) -> impl Stream<Item = anyhow::Result<u64>> {
    try_stream! {
        let mut from = from;
        loop {
            from = height_tracker.wait(from).await?;
            yield from;
            from += 1;
        }
    }
}

struct ForkNavigator<C, F>
where
    C: Fn(evm::BlockId) -> F,
    F: Future<Output = anyhow::Result<Block>>,
{
    chain: Vec<HashAndHeight>,
    get_block: C,
}

impl<C, F> ForkNavigator<C, F>
where
    C: Fn(evm::BlockId) -> F,
    F: Future<Output = anyhow::Result<Block>>,
{
    pub fn new(state: HashAndHeight, get_block: C) -> ForkNavigator<C, F> {
        ForkNavigator {
            chain: vec![state],
            get_block,
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
            let new_block = (self.get_block)(best.into()).await?;
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
                let hash = best_head.hash.parse::<evm::H256>()?;
                let block = (self.get_block)(hash.into()).await?;
                best_head = HashAndHeight {
                    hash: block.header.parent_hash.clone(),
                    height: block.header.number - 1,
                };
                new_blocks.push(block);
            }

            while chain.last().unwrap().hash != best_head.hash {
                let hash = best_head.hash.parse::<evm::H256>()?;
                let block = (self.get_block)(hash.into()).await?;
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

struct HeightTracker {
    tx: mpsc::UnboundedSender<(u128, oneshot::Sender<anyhow::Result<u64>>)>,
    interval: Duration,
}

impl HeightTracker {
    fn new(client: Provider<Http>, interval: Duration) -> HeightTracker {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<(u128, oneshot::Sender<anyhow::Result<u64>>)>();

        tokio::spawn(async move {
            let mut last_height = 0;
            let mut last_access = 0;
            let interval = interval.as_millis();

            while let Some((time, tx)) = rx.recv().await {
                let diff = time.saturating_sub(last_access);

                if diff > interval {
                    last_height = match client.get_block_number().await {
                        Ok(number) => number.as_u64(),
                        Err(e) => {
                            tx.send(Err(e.into())).expect("the receiver dropped");
                            continue;
                        }
                    };
                    last_access = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(now) => now.as_millis(),
                        Err(e) => {
                            tx.send(Err(e.into())).expect("the receiver dropped");
                            continue;
                        }
                    };
                }

                tx.send(Ok(last_height)).expect("the receiver dropped");
            }
        });

        HeightTracker { tx, interval }
    }

    async fn height(&self) -> anyhow::Result<u64> {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let (tx, rx) = oneshot::channel();
        self.tx.send((now, tx)).expect("the receiver dropped");
        let height = rx.await.expect("the sender dropped")?;
        Ok(height)
    }

    async fn wait(&self, height: u64) -> anyhow::Result<u64> {
        let mut current = self.height().await?;
        while current < height {
            tokio::time::sleep(self.interval).await;
            current = self.height().await?;
        }
        Ok(current)
    }
}
