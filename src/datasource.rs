use futures_core::stream::Stream;

#[derive(Debug, Clone, Default)]
pub struct LogRequest {
    pub address: Vec<String>,
    pub topic0: Vec<String>,
    pub transaction: bool,
    pub transaction_traces: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TxRequest {
    pub address: Vec<String>,
    pub sighash: Vec<String>,
    pub traces: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TraceRequest {
    pub address: Vec<String>,
    pub sighash: Vec<String>,
    pub transaction: bool,
    pub parents: bool,
}

#[derive(Debug, Clone)]
pub struct DataRequest {
    pub from: u64,
    pub to: Option<u64>,
    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TxRequest>,
    pub traces: Vec<TraceRequest>,
}

#[derive(Debug)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub size: u64,
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

#[derive(Debug)]
pub struct Transaction {
    pub transaction_index: u32,
    pub hash: String,
    pub nonce: u64,
    pub from: String,
    pub to: Option<String>,
    pub input: String,
    pub value: String,
    pub gas: String,
    pub gas_price: String,
    pub max_fee_per_gas: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub v: String,
    pub r: String,
    pub s: String,
    pub y_parity: Option<u8>,
    pub gas_used: String,
    pub cumulative_gas_used: String,
    pub effective_gas_price: String,
    pub r#type: i32,
    pub status: i32,
}

#[derive(Debug)]
pub struct Log {
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
    pub log_index: u32,
    pub transaction_index: u32,
}

#[derive(Debug)]
pub enum TraceType {
    Create,
    Call,
    Suicide,
    Reward,
}

#[derive(Debug)]
pub enum CallType {
    Call,
    Callcode,
    Delegatecall,
    Staticcall,
}

#[derive(Debug)]
pub struct TraceAction {
    pub from: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub r#type: Option<CallType>,
}

#[derive(Clone, Debug)]
pub struct TraceResult {
    pub gas_used: Option<String>,
    pub address: Option<String>,
    pub output: Option<String>,
}

#[derive(Debug)]
pub struct Trace {
    pub transaction_index: u32,
    pub r#type: TraceType,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    pub action: Option<TraceAction>,
    pub result: Option<TraceResult>,
}

#[derive(Debug)]
pub struct Block {
    pub header: BlockHeader,
    pub logs: Vec<Log>,
    pub transactions: Vec<Transaction>,
    pub traces: Vec<Trace>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct HashAndHeight {
    pub hash: String,
    pub height: u64,
}

impl From<&Block> for HashAndHeight {
    fn from(value: &Block) -> Self {
        HashAndHeight {
            hash: value.header.hash.clone(),
            height: value.header.number,
        }
    }
}

pub struct HotUpdate {
    pub blocks: Vec<Block>,
    pub base_head: HashAndHeight,
    pub finalized_head: HashAndHeight,
}

pub type BlockStream = Box<dyn Stream<Item = anyhow::Result<Vec<Block>>> + Send>;

pub type HotBlockStream = Box<dyn Stream<Item = anyhow::Result<HotUpdate>> + Send>;

#[async_trait::async_trait]
pub trait DataSource {
    fn get_finalized_blocks(
        &self,
        request: DataRequest,
        stop_on_head: bool,
    ) -> anyhow::Result<BlockStream>;
    async fn get_finalized_height(&self) -> anyhow::Result<u64>;
    async fn get_block_hash(&self, height: u64) -> anyhow::Result<String>;
}

#[async_trait::async_trait]
pub trait HotSource: DataSource {
    fn get_hot_blocks(
        &self,
        request: DataRequest,
        state: HashAndHeight,
    ) -> anyhow::Result<HotBlockStream>;
    fn as_ds(&self) -> &(dyn DataSource + Send + Sync);
}

pub trait HotDataSource: DataSource + HotSource {}
