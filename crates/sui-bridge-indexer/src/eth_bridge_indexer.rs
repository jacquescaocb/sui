// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use ethers::prelude::Transaction;
use ethers::providers::{Http, Middleware, Provider, StreamExt, Ws};
use ethers::types::{Address as EthAddress, Block, Filter, H256};
use sui_bridge::error::BridgeError;
use sui_bridge::eth_client::EthClient;
use sui_bridge::eth_syncer::EthSyncer;
use sui_bridge::metered_eth_provider::MeteredEthHttpProvier;
use sui_bridge::retry_with_max_elapsed_time;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tracing::info;

use mysten_metrics::spawn_monitored_task;
use sui_bridge::abi::{EthBridgeEvent, EthSuiBridgeEvents};

use crate::metrics::BridgeIndexerMetrics;
use sui_bridge::metrics::BridgeMetrics;
use sui_bridge::types::{EthEvent, RawEthLog};
use sui_indexer_builder::indexer_builder::{DataMapper, DataSender, Datasource};

use crate::{
    BridgeDataSource, ProcessedTxnData, TokenTransfer, TokenTransferData, TokenTransferStatus,
};

type RawEthData = (RawEthLog, Block<H256>, Transaction);

pub struct EthSubscriptionDatasource {
    addresses: Vec<EthAddress>,
    eth_ws_url: String,
    indexer_metrics: BridgeIndexerMetrics,
}

impl EthSubscriptionDatasource {
    pub fn new(
        eth_sui_bridge_contract_addresses: Vec<String>,
        eth_ws_url: String,
        indexer_metrics: BridgeIndexerMetrics,
    ) -> Result<Self, anyhow::Error> {
        let bridge_addresses = eth_sui_bridge_contract_addresses
            .iter()
            .map(|address| EthAddress::from_str(address).unwrap())
            .collect();
        Ok(Self {
            addresses: bridge_addresses,
            eth_ws_url,
            indexer_metrics,
        })
    }
}
#[async_trait]
impl Datasource<RawEthData> for EthSubscriptionDatasource {
    async fn start_data_retrieval(
        &self,
        starting_checkpoint: u64,
        target_checkpoint: u64,
        data_sender: DataSender<RawEthData>,
    ) -> Result<JoinHandle<Result<(), Error>>, Error> {
        let filter = Filter::new()
            .address(self.addresses.clone())
            .from_block(starting_checkpoint)
            .to_block(target_checkpoint);

        let eth_ws_url = self.eth_ws_url.clone();
        let indexer_metrics: BridgeIndexerMetrics = self.indexer_metrics.clone();

        let handle = spawn_monitored_task!(async move {
            let eth_ws_client = Provider::<Ws>::connect(&eth_ws_url).await?;

            let mut cached_blocks: HashMap<u64, Block<H256>> = HashMap::new();

            let mut stream = eth_ws_client.subscribe_logs(&filter).await?;
            while let Some(log) = stream.next().await {
                let raw_log = RawEthLog {
                    block_number: log
                        .block_number
                        .ok_or(BridgeError::ProviderError(
                            "Provider returns log without block_number".into(),
                        ))
                        .unwrap()
                        .as_u64(),
                    tx_hash: log
                        .transaction_hash
                        .ok_or(BridgeError::ProviderError(
                            "Provider returns log without transaction_hash".into(),
                        ))
                        .unwrap(),
                    log,
                };

                let block_number = raw_log.block_number();

                let block = if let Some(cached_block) = cached_blocks.get(&block_number) {
                    cached_block.clone()
                } else {
                    let Ok(Ok(Some(block))) = retry_with_max_elapsed_time!(
                        eth_ws_client.get_block(block_number),
                        Duration::from_secs(30000)
                    ) else {
                        panic!("Unable to get block from provider");
                    };

                    cached_blocks.insert(block_number, block.clone());
                    block
                };

                let Ok(Ok(Some(transaction))) = retry_with_max_elapsed_time!(
                    eth_ws_client.get_transaction(raw_log.tx_hash),
                    Duration::from_secs(30000)
                ) else {
                    panic!("Unable to get transaction from provider");
                };

                data_sender
                    .send((block_number, vec![(raw_log, block, transaction)]))
                    .await?;

                indexer_metrics
                    .latest_committed_eth_block
                    .set(block_number as i64);
            }

            Ok::<_, Error>(())
        });
        Ok(handle)
    }
}

pub struct EthFinalizedSyncDatasource {
    bridge_addresses: Vec<EthAddress>,
    eth_http_url: String,
    indexer_metrics: BridgeIndexerMetrics,
    bridge_metrics: Arc<BridgeMetrics>,
}

impl EthFinalizedSyncDatasource {
    pub fn new(
        eth_sui_bridge_contract_addresses: Vec<String>,
        eth_http_url: String,
        indexer_metrics: BridgeIndexerMetrics,
        bridge_metrics: Arc<BridgeMetrics>,
    ) -> Result<Self, anyhow::Error> {
        let bridge_addresses = eth_sui_bridge_contract_addresses
            .iter()
            .map(|address| EthAddress::from_str(address).unwrap())
            .collect();
        Ok(Self {
            bridge_addresses,
            eth_http_url,
            indexer_metrics,
            bridge_metrics,
        })
    }
}
#[async_trait]
impl Datasource<RawEthData> for EthFinalizedSyncDatasource {
    async fn start_data_retrieval(
        &self,
        starting_checkpoint: u64,
        target_checkpoint: u64,
        data_sender: DataSender<RawEthData>,
    ) -> Result<JoinHandle<Result<(), Error>>, Error> {
        let client: Arc<EthClient<MeteredEthHttpProvier>> = Arc::new(
            EthClient::<MeteredEthHttpProvier>::new(
                &self.eth_http_url,
                HashSet::from_iter(self.bridge_addresses.clone()),
                self.bridge_metrics.clone(),
            )
            .await?,
        );

        let provider = Arc::new(
            Provider::<Http>::try_from(&self.eth_http_url)?
                .interval(std::time::Duration::from_millis(2000)),
        );

        let bridge_addresses = self.bridge_addresses.clone();
        let indexer_metrics: BridgeIndexerMetrics = self.indexer_metrics.clone();
        let client = Arc::clone(&client);
        let provider = Arc::clone(&provider);
        let bridge_metrics = Arc::clone(&self.bridge_metrics);
        let current_block = provider.get_block_number().await?.as_u64();

        let handle = spawn_monitored_task!(async move {
            if target_checkpoint > current_block {
                retrieve_and_process_live_finalized_logs(
                    client,
                    provider,
                    bridge_addresses,
                    starting_checkpoint,
                    data_sender,
                    indexer_metrics,
                    bridge_metrics,
                )
                .await;
            } else {
                retrieve_and_process_log_range(
                    client,
                    provider,
                    bridge_addresses,
                    starting_checkpoint,
                    target_checkpoint,
                    data_sender,
                    indexer_metrics,
                )
                .await?;
            }
            Ok::<_, Error>(())
        });

        Ok(handle)
    }
}

async fn retrieve_and_process_live_finalized_logs(
    client: Arc<EthClient<MeteredEthHttpProvier>>,
    provider: Arc<Provider<Http>>,
    addresses: Vec<EthAddress>,
    starting_checkpoint: u64,
    data_sender: DataSender<RawEthData>,
    indexer_metrics: BridgeIndexerMetrics,
    bridge_metrics: Arc<BridgeMetrics>,
) {
    let eth_contracts_to_watch = HashMap::from_iter(
        addresses
            .iter()
            .map(|address| (*address, starting_checkpoint)),
    );

    let (_, mut eth_events_rx, _) = EthSyncer::new(client.clone(), eth_contracts_to_watch)
        .run(bridge_metrics.clone())
        .await
        .expect("Failed to start eth syncer");

    // forward received events to the data sender
    while let Some((_, block, logs)) = eth_events_rx.recv().await {
        let raw_logs: Vec<RawEthLog> = logs
            .into_iter()
            .map(|log| RawEthLog {
                block_number: block,  // Handle block number conversion
                tx_hash: log.tx_hash, // Assuming `event` contains `tx_hash`
                log: log.log,
            })
            .collect();

        process_logs(raw_logs, provider.clone(), data_sender.clone(), block)
            .await
            .expect("Failed to process logs");
        indexer_metrics.latest_committed_eth_block.set(block as i64);
    }

    panic!("Eth syncer stopped unexpectedly");
}

async fn retrieve_and_process_log_range(
    client: Arc<EthClient<MeteredEthHttpProvier>>,
    provider: Arc<Provider<Http>>,
    addresses: Vec<EthAddress>,
    starting_checkpoint: u64,
    target_checkpoint: u64,
    data_sender: DataSender<RawEthData>,
    indexer_metrics: BridgeIndexerMetrics,
) -> Result<(), Error> {
    let Ok(Ok(logs)) = retry_with_max_elapsed_time!(
        client.get_raw_events_in_range(addresses.clone(), starting_checkpoint, target_checkpoint),
        Duration::from_secs(30000)
    ) else {
        panic!("Unable to get logs from provider");
    };

    process_logs(
        logs,
        provider.clone(),
        data_sender.clone(),
        target_checkpoint,
    )
    .await?;

    indexer_metrics
        .last_synced_eth_block
        .set(target_checkpoint as i64);

    Ok::<_, Error>(())
}

async fn process_logs(
    logs: Vec<RawEthLog>,
    provider: Arc<Provider<Http>>,
    data_sender: DataSender<RawEthData>,
    target_checkpoint: u64,
) -> Result<(), Error> {
    let mut data = Vec::new();
    let mut cached_blocks: HashMap<u64, Block<H256>> = HashMap::new();
    for log in logs {
        let block = if let Some(cached_block) = cached_blocks.get(&log.block_number) {
            cached_block.clone()
        } else {
            let Ok(Ok(Some(block))) = retry_with_max_elapsed_time!(
                provider.get_block(log.block_number),
                Duration::from_secs(30000)
            ) else {
                panic!("Unable to get block from provider");
            };

            cached_blocks.insert(log.block_number, block.clone());
            block
        };

        let Ok(Ok(Some(transaction))) = retry_with_max_elapsed_time!(
            provider.get_transaction(log.tx_hash),
            Duration::from_secs(30000)
        ) else {
            panic!("Unable to get transaction from provider");
        };

        data.push((log, block, transaction));
    }

    data_sender.send((target_checkpoint, data)).await?;

    Ok::<_, Error>(())
}

#[derive(Clone)]
pub struct EthDataMapper {
    pub metrics: BridgeIndexerMetrics,
}

impl<E: EthEvent> DataMapper<(E, Block<H256>, Transaction), ProcessedTxnData> for EthDataMapper {
    fn map(
        &self,
        (log, block, transaction): (E, Block<H256>, Transaction),
    ) -> Result<Vec<ProcessedTxnData>, Error> {
        let eth_bridge_event = EthBridgeEvent::try_from_log(log.log());
        if eth_bridge_event.is_none() {
            return Ok(vec![]);
        }
        self.metrics.total_eth_bridge_transactions.inc();
        let bridge_event = eth_bridge_event.unwrap();
        let timestamp_ms = block.timestamp.as_u64() * 1000;
        let gas = transaction.gas;

        let transfer = match bridge_event {
            EthBridgeEvent::EthSuiBridgeEvents(bridge_event) => match bridge_event {
                EthSuiBridgeEvents::TokensDepositedFilter(bridge_event) => {
                    info!("Observed Eth Deposit at block: {}", log.block_number());
                    self.metrics.total_eth_token_deposited.inc();
                    ProcessedTxnData::TokenTransfer(TokenTransfer {
                        chain_id: bridge_event.source_chain_id,
                        nonce: bridge_event.nonce,
                        block_height: log.block_number(),
                        timestamp_ms,
                        txn_hash: transaction.hash.as_bytes().to_vec(),
                        txn_sender: bridge_event.sender_address.as_bytes().to_vec(),
                        status: TokenTransferStatus::Deposited,
                        gas_usage: gas.as_u64() as i64,
                        data_source: BridgeDataSource::Eth,
                        data: Some(TokenTransferData {
                            sender_address: bridge_event.sender_address.as_bytes().to_vec(),
                            destination_chain: bridge_event.destination_chain_id,
                            recipient_address: bridge_event.recipient_address.to_vec(),
                            token_id: bridge_event.token_id,
                            amount: bridge_event.sui_adjusted_amount,
                        }),
                    })
                }
                EthSuiBridgeEvents::TokensClaimedFilter(bridge_event) => {
                    info!("Observed Eth Claim at block: {}", log.block_number());
                    self.metrics.total_eth_token_transfer_claimed.inc();
                    ProcessedTxnData::TokenTransfer(TokenTransfer {
                        chain_id: bridge_event.source_chain_id,
                        nonce: bridge_event.nonce,
                        block_height: log.block_number(),
                        timestamp_ms,
                        txn_hash: transaction.hash.as_bytes().to_vec(),
                        txn_sender: bridge_event.sender_address.to_vec(),
                        status: TokenTransferStatus::Claimed,
                        gas_usage: gas.as_u64() as i64,
                        data_source: BridgeDataSource::Eth,
                        data: None,
                    })
                }
                EthSuiBridgeEvents::PausedFilter(_)
                | EthSuiBridgeEvents::UnpausedFilter(_)
                | EthSuiBridgeEvents::UpgradedFilter(_)
                | EthSuiBridgeEvents::InitializedFilter(_) => {
                    // TODO: handle these events
                    self.metrics.total_eth_bridge_txn_other.inc();
                    return Ok(vec![]);
                }
            },
            EthBridgeEvent::EthBridgeCommitteeEvents(_)
            | EthBridgeEvent::EthBridgeLimiterEvents(_)
            | EthBridgeEvent::EthBridgeConfigEvents(_)
            | EthBridgeEvent::EthCommitteeUpgradeableContractEvents(_) => {
                // TODO: handle these events
                self.metrics.total_eth_bridge_txn_other.inc();
                return Ok(vec![]);
            }
        };
        Ok(vec![transfer])
    }
}
