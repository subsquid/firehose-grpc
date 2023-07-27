use serde::{Serialize, Deserialize};
use serde_json::json;

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
pub struct Block {
    pub header: BlockHeader,
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

    pub async fn query(&self, from_block: u64, to_block: Option<u64>) -> Result<Vec<Block>, reqwest::Error> {
        let worker_url = self.client.get(format!("https://v2.archive.subsquid.io/network/ethereum-mainnet/{}/worker", from_block))
            .send()
            .await?
            .text()
            .await?;

        let mut query = json!({
            "fromBlock": from_block,
            "fields": {
                "block": {
                    "number": true,
                    "hash": true,
                    "parentHash": true,
                    "difficulty": true,
                    "totalDifficulty": true,
                    "size": true,
                    "sha3Uncles": true,
                    "gasLimit": true,
                    "gasUsed": true,
                    "timestamp": true,
                    "miner": true,
                    "stateRoot": true,
                    "transactionsRoot": true,
                    "receiptsRoot": true,
                    "logsBloom": true,
                    "extraData": true,
                    "mixHash": true,
                    "baseFeePerGas": true,
                    "nonce": true
                }
            }
        });
        if let Some(to_block) = to_block {
            query.as_object_mut().unwrap().insert("toBlock".to_string(), to_block.into());
        }

        let response = self.client.post(worker_url)
            .json(&query)
            .send()
            .await?;

        if let Err(err) = response.error_for_status_ref() {
            let text = response.text().await?;
            dbg!(text);
            return Err(err)
        }

        let blocks: Vec<Block> = response
            .json()
            .await?;
        Ok(blocks)
    }
}
