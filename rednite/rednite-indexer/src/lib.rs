// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

mod fetcher;

use std::sync::Arc;

use aptos_api::context::Context;
use aptos_config::config::NodeConfig;
use aptos_logger::info;
use aptos_mempool::MempoolClientSender;
use aptos_types::chain_id::ChainId;
use storage_interface::DbReader;
use tokio::runtime::{Builder, Runtime};

use crate::fetcher::{TransactionFetcher, TransactionFetcherOptions};

pub fn bootstrap(
    config: &NodeConfig,
    chain_id: ChainId,
    db: Arc<dyn DbReader>,
    mp_sender: MempoolClientSender,
) -> Option<anyhow::Result<Runtime>> {
    let runtime = Builder::new_multi_thread()
        .thread_name("indexeer")
        .disable_lifo_slot()
        .enable_all()
        .build()
        .expect("[indexer] failed to create runtime");

    let node_config = config.clone();

    runtime.spawn(async move {
        let context = Context::new(chain_id, db, mp_sender, node_config);
        let context = Arc::new(context);

        run_forever(context).await;
    });

    Some(Ok(runtime))
}

pub async fn run_forever(context: Arc<Context>) {
    info!("Starting indexer...");

    let batch_size = 500;
    let fetch_tasks = 5;

    let options =
        TransactionFetcherOptions::new(None, None, Some(batch_size), None, fetch_tasks as usize);

    let resolver = Arc::new(context.move_resolver().unwrap());
    let mut transaction_fetcher = TransactionFetcher::new(context, resolver, 0, options);

    // Add fetching
    info!("Starting transaction fetcher...");
    let start_version = 0;
    transaction_fetcher.set_version(start_version).await;
    transaction_fetcher.start().await;

    info!("Starting transaction indexer loop...");

    let mut i = 0;
    loop {
        let transactions = transaction_fetcher.fetch_next_batch().await;

        for transaction in &transactions {
            info!(
                "#{} handle transaction {}",
                i,
                transaction.transaction_info().unwrap().hash
            );

            i += 1;
        }
    }
}
