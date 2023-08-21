use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    pub address: Vec<String>,
    pub topic0: Vec<String>,
    pub topic1: Vec<String>,
    pub topic2: Vec<String>,
    pub topic3: Vec<String>,
    pub transaction: bool,
    pub transaction_traces: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockFieldSelection {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub size: bool,
    pub sha3_uncles: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub miner: bool,
    pub state_root: bool,
    pub transactions_root: bool,
    pub receipts_root: bool,
    pub logs_bloom: bool,
    pub extra_data: bool,
    pub mix_hash: bool,
    pub base_fee_per_gas: bool,
    pub nonce: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogFieldSelection {
    pub log_index: bool,
    pub transaction_index: bool,
    pub address: bool,
    pub data: bool,
    pub topics: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TxFieldSelection {
    pub transaction_index: bool,
    pub hash: bool,
    pub nonce: bool,
    pub from: bool,
    pub to: bool,
    pub input: bool,
    pub value: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub max_fee_per_gas: bool,
    pub max_priority_fee_per_gas: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub y_parity: bool,
    pub gas_used: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    pub r#type: bool,
    pub status: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TraceFieldSelection {
    pub transaction_index: bool,
    pub r#type: bool,
    pub error: bool,
    pub revert_reason: bool,
    pub create_from: bool,
    pub create_value: bool,
    pub create_gas: bool,
    pub create_result_gas_used: bool,
    pub create_result_address: bool,
    pub call_from: bool,
    pub call_to: bool,
    pub call_value: bool,
    pub call_gas: bool,
    pub call_input: bool,
    pub call_type: bool,
    pub call_result_gas_used: bool,
    pub call_result_output: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FieldSelection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockFieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<LogFieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TxFieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<TraceFieldSelection>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BatchRequest {
    pub from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<FieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<Vec<LogRequest>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
    pub log_index: u32,
    pub transaction_index: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TraceType {
    Create,
    Call,
    Suicide,
    Reward,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum CallType {
    Call,
    Callcode,
    Delegatecall,
    Staticcall,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub transaction_index: u32,
    pub r#type: TraceType,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    pub create_from: Option<String>,
    pub create_value: Option<String>,
    pub create_gas: Option<String>,
    pub create_result_gas_used: Option<String>,
    pub create_result_address: Option<String>,
    pub call_from: Option<String>,
    pub call_to: Option<String>,
    pub call_value: Option<String>,
    pub call_gas: Option<String>,
    pub call_input: Option<String>,
    pub call_type: Option<CallType>,
    pub call_result_gas_used: Option<String>,
    pub call_result_output: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Block {
    pub header: BlockHeader,
    pub logs: Option<Vec<Log>>,
    pub transactions: Option<Vec<Transaction>>,
    pub traces: Option<Vec<Trace>>,
}

#[derive(Debug)]
pub enum Error {
    Http(reqwest::Error),
    Parse(serde_json::Error),
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Http(err)
    }
}

#[derive(Debug)]
pub struct Archive {
    client: reqwest::Client,
}

impl Archive {
    pub fn new() -> Archive {
        let client = reqwest::Client::new();
        Archive { client }
    }

    pub async fn height(&self) -> Result<u64, Error> {
        let response = self
            .client
            .get("https://v2.archive.subsquid.io/network/ethereum-mainnet/height")
            .send()
            .await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(Error::Http(err));
        }

        let text = response.text().await?;
        serde_json::from_str(&text).map_err(|err| {
            dbg!(text);
            Error::Parse(err)
        })
    }

    pub async fn query(&self, request: &BatchRequest) -> Result<Vec<Block>, Error> {
        let worker_url = self.worker(request.from_block).await?;
        let response = self.client.post(worker_url).json(&request).send().await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(Error::Http(err));
        }

        let text = response.text().await?;
        serde_json::from_str(&text).map_err(|err| {
            dbg!(text);
            Error::Parse(err)
        })
    }

    pub async fn worker(&self, start_block: u64) -> Result<String, Error> {
        let response = self
            .client
            .get(format!(
                "https://v2.archive.subsquid.io/network/ethereum-mainnet/{}/worker",
                start_block
            ))
            .send()
            .await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(Error::Http(err));
        }

        response.text().await.map_err(|err| Error::Http(err))
    }
}
