use serde::{Deserialize, Serialize};

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
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction_logs: bool,
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
    pub transaction_logs: bool,
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
pub struct Query {
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
