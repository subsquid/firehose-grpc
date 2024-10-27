use std::sync::Arc;

use anyhow::Context;
use prost::Message;
use tokio_stream::StreamExt;

use firehose_grpc::portal::Portal;
use firehose_grpc::ds_portal::PortalDataSource;
use firehose_grpc::firehose::Firehose;
use firehose_grpc::pbcodec::Block;
use firehose_grpc::pbfirehose::{Request, SingleBlockRequest};
use firehose_grpc::pbfirehose::single_block_request::{Reference, BlockNumber};
use firehose_grpc::pbtransforms::{CombinedFilter, CallToFilter, LogFilter};

struct TestFirehose {
    firehose: Firehose
}

impl TestFirehose {
    fn new() -> TestFirehose {
        let url = "https://portal.sqd.dev/datasets/ethereum-mainnet".into();
        let portal = Arc::new(Portal::new(url));
        let portal_ds = Arc::new(PortalDataSource::new(portal));
        let firehose = Firehose::new(portal_ds, None);
        TestFirehose { firehose }
    }

    async fn blocks(&self, req: &Request) -> anyhow::Result<Vec<Block>> {
        let mut blocks = vec![];
        let stream = self.firehose.blocks(&req).await?;
        tokio::pin!(stream);

        while let Some(resp) = stream.try_next().await? {
            let data = resp.block.context("no block data")?;
            let block = Block::decode(&data.value[..])?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    async fn block(&self, req: &SingleBlockRequest) -> anyhow::Result<Block> {
        let resp = self.firehose.block(req).await?;
        let data = resp.block.context("no block data")?;
        let block = Block::decode(&data.value[..])?;
        Ok(block)
    }
}

#[tokio::test]
async fn test_call_filters() -> Result<(), anyhow::Error> {
    let filter = CombinedFilter {
        call_filters: vec![CallToFilter {
            addresses: vec![prefix_hex::decode("0xb8901acb165ed027e32754e0ffe830802919727f")?],
            signatures: vec![],
        }],
        log_filters: vec![],
        send_all_block_headers: false
    };
    let req = Request {
        cursor: "".into(),
        final_blocks_only: false,
        start_block_num: 20000000,
        stop_block_num: 20000000,
        transforms: vec![prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.transform.v1.CombinedFilter".to_string(),
            value: filter.encode_to_vec()
        }],
    };

    let firehose = TestFirehose::new();
    let blocks = firehose.blocks(&req).await?;

    let block = &blocks[0];
    let trace = &block.transaction_traces[0];
    let hash = prefix_hex::encode(&trace.hash);
    let to = prefix_hex::encode(&trace.calls[0].address);
    assert_eq!(blocks.len(), 1);
    assert_eq!(block.transaction_traces.len(), 1);
    assert_eq!(trace.calls.len(), 1);
    assert_eq!(hash, "0x8400bdadaf54699b14c802daaef902155be80cb2e4406f20107a5f86b042de04");
    assert_eq!(to, "0xb8901acb165ed027e32754e0ffe830802919727f");

    Ok(())
}

#[tokio::test]
async fn test_log_filters() -> Result<(), anyhow::Error> {
    let usdt_address = "0xdac17f958d2ee523a2206206994597c13d831ec7";
    let transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    let filter = CombinedFilter {
        call_filters: vec![],
        log_filters: vec![LogFilter {
            addresses: vec![prefix_hex::decode(usdt_address)?],
            event_signatures: vec![prefix_hex::decode(transfer_topic)?]
        }],
        send_all_block_headers: false
    };
    let req = Request {
        cursor: "".into(),
        final_blocks_only: false,
        start_block_num: 20000000,
        stop_block_num: 20000000,
        transforms: vec![prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.transform.v1.CombinedFilter".to_string(),
            value: filter.encode_to_vec()
        }],
    };

    let firehose = TestFirehose::new();
    let blocks = firehose.blocks(&req).await?;

    let block = &blocks[0];
    assert_eq!(blocks.len(), 1);
    assert_eq!(block.transaction_traces.len(), 9);
    'outer: for tx in &block.transaction_traces {
        let receipt = tx.receipt.as_ref().unwrap();
        for log in &receipt.logs {
            let address = prefix_hex::encode(&log.address);
            let topic = prefix_hex::encode(&log.topics[0]);
            if address == usdt_address && topic == transfer_topic {
                continue 'outer
            }
        }
        panic!("tx has no usdt logs");
    }

    Ok(())
}

#[tokio::test]
async fn test_cursor() -> Result<(), anyhow::Error> {
    let filter = CombinedFilter {
        call_filters: vec![],
        log_filters: vec![LogFilter {
            addresses: vec![prefix_hex::decode("0xdac17f958d2ee523a2206206994597c13d831ec7")?],
            event_signatures: vec![prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")?]
        }],
        send_all_block_headers: false
    };
    let req = Request {
        cursor: "19999999:0xb390d63aac03bbef75de888d16bd56b91c9291c2a7e38d36ac24731351522bd1:20000000:0xd24fd73f794058a3807db926d8898c6481e902b7edb91ce0d479d6760f276183".into(),
        final_blocks_only: false,
        start_block_num: 19000000,
        stop_block_num: 20000000,
        transforms: vec![prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.transform.v1.CombinedFilter".to_string(),
            value: filter.encode_to_vec()
        }],
    };

    let firehose = TestFirehose::new();
    let blocks = firehose.blocks(&req).await?;

    let block = &blocks[0];
    assert_eq!(blocks.len(), 1);
    assert_eq!(block.number, 20000000);

    Ok(())
}

#[tokio::test]
async fn test_single_block_request() -> Result<(), anyhow::Error> {
    let block_num = BlockNumber { num: 20000000 };
    let req = SingleBlockRequest {
        reference: Some(Reference::BlockNumber(block_num)),
        transforms: vec![]
    };

    let firehose = TestFirehose::new();
    let block = firehose.block(&req).await?;

    assert_eq!(block.number, 20000000);

    Ok(())
}
