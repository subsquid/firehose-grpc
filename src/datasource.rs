use futures_core::stream::Stream;

#[derive(Debug)]
pub struct LogRequest {
    pub address: Vec<String>,
    pub topic0: Vec<String>,
}

#[derive(Debug)]
pub struct TransactionRequest {}

#[derive(Debug)]
pub struct DataRequest {
    pub from: u64,
    pub to: Option<u64>,
    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TransactionRequest>,
}

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

pub struct Log {
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
    pub log_index: u32,
    pub transaction_index: u32,
}

pub enum TraceType {
    Create,
    Call,
    Suicide,
    Reward,
}

pub enum CallType {
    Call,
    Callcode,
    Delegatecall,
    Staticcall,
}

pub struct TraceAction {
    pub from: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub r#type: Option<CallType>,
}

#[derive(Clone)]
pub struct TraceResult {
    pub gas_used: Option<String>,
    pub address: Option<String>,
    pub output: Option<String>,
}

pub struct Trace {
    pub transaction_index: u32,
    pub r#type: TraceType,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    pub action: Option<TraceAction>,
    pub result: Option<TraceResult>,
}

pub struct Block {
    pub header: BlockHeader,
    pub logs: Vec<Log>,
    pub transactions: Vec<Transaction>,
    pub traces: Vec<Trace>,
}

pub type BlockStream = Box<dyn Stream<Item = anyhow::Result<Vec<Block>>> + Send>;

#[async_trait::async_trait]
pub trait DataSource {
    fn get_finalized_blocks(&self, request: DataRequest) -> anyhow::Result<BlockStream>;
    async fn get_finalized_height(&self) -> anyhow::Result<u64>;
}
