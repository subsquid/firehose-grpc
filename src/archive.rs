use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct LogRequest {
    pub address: Vec<String>,
    pub topic0: Vec<String>,
    pub topic1: Vec<String>,
    pub topic2: Vec<String>,
    pub topic3: Vec<String>,
    pub transaction: bool,
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
pub struct FieldSelection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockFieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<LogFieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TxFieldSelection>,
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
pub struct Block {
    pub header: BlockHeader,
    pub logs: Option<Vec<Log>>,
    pub transactions: Option<Vec<Transaction>>
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

    pub async fn height(&self) -> Result<u64, reqwest::Error> {
        let height = self.client.get("https://v2.archive.subsquid.io/network/ethereum-mainnet/height")
            .send()
            .await?
            .text()
            .await?
            .parse()
            .unwrap();
        Ok(height)
    }

    pub async fn query(&self, request: &BatchRequest) -> Result<Vec<Block>, reqwest::Error> {
        let response = self.client.get(format!("https://v2.archive.subsquid.io/network/ethereum-mainnet/{}/worker", request.from_block))
            .send()
            .await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(err)
        }

        let worker_url = response.text().await?;

        let response = self.client.post(worker_url)
            .json(&request)
            .send()
            .await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(err)
        }

        let text = response.text().await?;
        let blocks = match serde_json::from_str(&text) {
            Ok(blocks) => blocks,
            Err(err) => {
                dbg!(text, err);
                panic!("decode error");
            }
        };

        Ok(blocks)
    }
}
