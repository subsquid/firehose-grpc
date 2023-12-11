use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use tracing::debug;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topic0: Vec<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction_traces: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TxRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub traces: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TraceRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_sighash: Vec<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub parents: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TxRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traces: Option<Vec<TraceRequest>>,
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
    pub timestamp: Number,
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
pub struct TraceAction {
    pub from: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub r#type: Option<CallType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TraceResult {
    pub gas_used: Option<String>,
    pub address: Option<String>,
    pub output: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub transaction_index: u32,
    pub r#type: TraceType,
    pub error: Option<String>,
    #[serde(default)]
    pub revert_reason: Option<String>,
    pub action: Option<TraceAction>,
    pub result: Option<TraceResult>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Block {
    pub header: BlockHeader,
    pub logs: Option<Vec<Log>>,
    pub transactions: Option<Vec<Transaction>>,
    pub traces: Option<Vec<Trace>>,
}

#[derive(Debug)]
pub struct Archive {
    client: Client,
    url: String,
}

impl Archive {
    pub fn new(url: String) -> Archive {
        let client = Client::new();
        Archive { client, url }
    }

    pub async fn height(&self) -> anyhow::Result<u64> {
        let response = self
            .client
            .get(format!("{}/height", self.url))
            .send()
            .await?;

        if let Err(_) = response.error_for_status_ref() {
            let text = response.text().await?;
            anyhow::bail!("failed response from archive - {}", text);
        }

        let text = response.text().await?;
        debug!("archive height {}", text);
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("serialization error - {}", e))
    }

    pub async fn query(&self, request: &BatchRequest) -> anyhow::Result<Vec<Block>> {
        debug!("archive query {:?}", request);
        let worker_url = self.worker(request.from_block).await?;
        let response = self.client.post(worker_url).json(&request).send().await?;

        if let Err(_) = response.error_for_status_ref() {
            let text = response.text().await?;
            anyhow::bail!("failed response from archive - {}", text);
        }

        let text = response.text().await?;
        debug!("archive query result {}", text);
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("serialization error - {}", e))
    }

    pub async fn worker(&self, start_block: u64) -> anyhow::Result<String> {
        let response = self
            .client
            .get(format!("{}/{}/worker", self.url, start_block))
            .send()
            .await?;

        if let Err(_) = response.error_for_status_ref() {
            let text = response.text().await?;
            anyhow::bail!("failed response from archive - {}", text);
        }

        let worker_url = response.text().await?;
        debug!("worker url {}", worker_url);
        Ok(worker_url)
    }
}
