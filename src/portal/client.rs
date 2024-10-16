use std::io::BufRead;

use futures_util::{TryStreamExt, Stream};
use prost::bytes::Buf;
use reqwest::Client;
use tracing::debug;

use crate::portal::query::Query;
use crate::portal::data::Block;

#[derive(Debug)]
pub struct Portal {
    client: Client,
    url: String,
}

impl Portal {
    pub fn new(url: String) -> Portal {
        let client = Client::new();
        Portal { client, url }
    }

    pub async fn height(&self) -> anyhow::Result<u64> {
        let url = format!("{}/height", self.url);
        let response = self.client.get(url).send().await?;

        if let Err(_) = response.error_for_status_ref() {
            let text = response.text().await?;
            anyhow::bail!("failed response from portal - {}", text);
        }

        let text = response.text().await?;
        debug!("portal height: {}", text);
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("serialization error - {}", e))
    }

    pub async fn stream(&self, query: &Query) -> anyhow::Result<impl Stream<Item = anyhow::Result<Block>>> {
        let url = format!("{}/stream", self.url);
        let response = self.client.post(url).json(query).send().await?;

        if let Err(_) = response.error_for_status_ref() {
            let text = response.text().await?;
            anyhow::bail!("failed response from portal - {}", text);
        }

        let mut stream = response.bytes_stream();
        let mut line = String::new();
        Ok(async_stream::try_stream! {
            while let Some(chunk) = stream.try_next().await? {
                let mut reader = chunk.reader();
                loop {
                    if 0 == reader.read_line(&mut line)? {
                        break
                    }

                    if let Some(last) = line.chars().last() {
                        if last != '\n' {
                            continue;
                        }
                    }

                    debug!("portal stream data: {}", line);
                    let block = serde_json::from_str(&line)?;
                    line.clear();
                    yield block
                }
            }
        })
    }
}
