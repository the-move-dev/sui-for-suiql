// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use itertools::Itertools;
use move_binary_format::CompiledModule;
use move_bytecode_utils::module_cache::GetModule;
use move_core_types::language_storage::ModuleId;
use mysten_metrics::{get_metrics, spawn_monitored_task};
use sui_rest_api::CheckpointData;
use sui_sdk::{SuiClientBuilder, SuiClient};
use sui_types::base_types::{ObjectRef, SuiAddress};
use sui_types::dynamic_field::DynamicFieldInfo;
use sui_types::dynamic_field::DynamicFieldName;
use sui_types::object::ObjectFormatOptions;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sui_types::dynamic_field::DynamicFieldType;
use sui_types::object::Object;

use std::collections::hash_map::Entry;
use std::collections::HashSet;
use sui_json_rpc::get_balance_changes_from_effect;
use sui_json_rpc::get_object_changes;
use sui_json_rpc::ObjectProvider;
use sui_json_rpc_types::{EndOfEpochInfo, SuiObjectDataOptions};
use sui_json_rpc_types::SuiMoveValue;
use sui_types::base_types::SequenceNumber;
use sui_types::digests::TransactionDigest;
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::event::SystemEpochInfoEvent;
use sui_types::object::{ObjectRead, Owner};
use sui_types::transaction::{TransactionData, TransactionDataAPI};
use tap::tap::TapFallible;
use tracing::{error, info, warn};

use sui_types::base_types::ObjectID;
use sui_types::messages_checkpoint::{CheckpointCommitment, CheckpointSequenceNumber};
use sui_types::sui_system_state::sui_system_state_summary::SuiSystemStateSummary;
use sui_types::sui_system_state::{get_sui_system_state, SuiSystemStateTrait};
use sui_types::SUI_SYSTEM_ADDRESS;

use crate::errors::IndexerError;
use crate::framework::interface::Handler;
use crate::metrics::IndexerMetrics;

use crate::store::{
    IndexerStoreV2, TemporaryCheckpointStore, TemporaryCheckpointStoreV2, TemporaryEpochStore,
    TransactionObjectChanges,
};
use crate::store::{InterimModuleResolver, TemporaryEpochStoreV2, TransactionObjectChangesV2};
use crate::types_v2::IndexedEpochInfo;
use crate::types_v2::{
    IndexedCheckpoint, IndexedEvent, IndexedTransaction, IndexerResult, TransactionKind, TxIndex,
};
use crate::types_v2::{IndexedEndOfEpochInfo, IndexedObject, IndexedPackage};
use crate::IndexerConfig;

const CHECKPOINT_QUEUE_SIZE: usize = 1000;

pub async fn new_handlers<S>(
    state: S,
    metrics: IndexerMetrics,
    config: &IndexerConfig,
) -> Result<CheckpointHandler<S>, IndexerError>
where
    S: IndexerStoreV2 + Clone + Sync + Send + 'static,
{
    let checkpoint_queue_size = std::env::var("CHECKPOINT_QUEUE_SIZE")
        .unwrap_or(CHECKPOINT_QUEUE_SIZE.to_string())
        .parse::<usize>()
        .unwrap();
    let global_metrics = get_metrics().unwrap();
    let (indexed_checkpoint_sender, indexed_checkpoint_receiver) =
        mysten_metrics::metered_channel::channel(
            checkpoint_queue_size,
            &global_metrics
                .channels
                .with_label_values(&["checkpoint_indexing"]),
        );

    let state_clone = state.clone();
    let metrics_clone = metrics.clone();
    let config_clone = config.clone();
    spawn_monitored_task!(start_tx_checkpoint_commit_task(
        state_clone,
        metrics_clone,
        config_clone,
        indexed_checkpoint_receiver,
    ));
    let sui_client = SuiClientBuilder::default().build(config.rpc_client_url.clone()).await
        .map_err(|e| IndexerError::FullNodeReadingError(e.to_string()))?;
    let checkpoint_processor = CheckpointHandler {
        state: state.clone(),
        metrics: metrics.clone(),
        indexed_checkpoint_sender,
        checkpoint_starting_tx_seq_numbers: HashMap::new(),
        object_cache: Arc::new(Mutex::new(InMemObjectCache::new())),
        sui_client: Arc::new(sui_client),
    };

    Ok(checkpoint_processor)
}

pub struct CheckpointHandler<S> {
    state: S,
    metrics: IndexerMetrics,
    indexed_checkpoint_sender: mysten_metrics::metered_channel::Sender<TemporaryCheckpointStoreV2>,
    // Map from checkpoint sequence number and its starting transaction sequence number
    checkpoint_starting_tx_seq_numbers: HashMap<CheckpointSequenceNumber, u64>,
    object_cache: Arc<Mutex<InMemObjectCache>>,
    sui_client: Arc<SuiClient>,
}

#[async_trait]
impl<S> Handler for CheckpointHandler<S>
where
    S: IndexerStoreV2 + Clone + Sync + Send + 'static,
{
    fn name(&self) -> &str {
        "checkpoint-handler"
    }

    async fn process_checkpoint(&mut self, checkpoint_data: &CheckpointData) -> anyhow::Result<()> {
        let checkpoint_seq = checkpoint_data.checkpoint_summary.sequence_number();
        info!(checkpoint_seq, "Checkpoint received by CheckpointHandler");

        // update next checkpoint starting tx seq number
        self.checkpoint_starting_tx_seq_numbers.insert(
            *checkpoint_seq + 1,
            checkpoint_data
                .checkpoint_summary
                .network_total_transactions
                + 1,
        );
        let current_checkpoint_starting_tx_seq = if checkpoint_seq == &0 {
            0
        } else if self
            .checkpoint_starting_tx_seq_numbers
            .contains_key(checkpoint_seq)
        {
            self.checkpoint_starting_tx_seq_numbers[checkpoint_seq]
        } else {
            self.state.get_checkpoint_ending_tx_sequence_number(checkpoint_seq - 1).await?
            .unwrap_or_else(|| {
                panic!("While processing checkpoint {}, we failed to find the starting tx seq both in mem and DB.", checkpoint_seq)
            }) + 1
        };

        // TODO: change to trace
        info!(
            checkpoint_seq,
            "Checkpoint starting tx sequence number: {current_checkpoint_starting_tx_seq}"
        );

        // Index checkpoint data
        let index_timer = self.metrics.checkpoint_index_latency.start_timer();

        let (checkpoint, epoch) = Self::index_checkpoint_and_epoch(
            &self.state,
            current_checkpoint_starting_tx_seq,
            checkpoint_data.clone(),
            self.object_cache.clone(),
            self.sui_client.clone(),
        )
        .await
        .tap_err(|e| {
            error!(
                checkpoint_seq,
                "Failed to index checkpoints with error: {}",
                e.to_string()
            );
        })?;
        let elapsed = index_timer.stop_and_record();

        // FIXME incorporate epoch change to TempStore

        // commit first epoch immediately, send other epochs to channel to be committed later.
        // if let Some(epoch) = epoch {
        //     if epoch.last_epoch.is_none() {
        //         let epoch_db_guard = self.metrics.epoch_db_commit_latency.start_timer();
        //         info!("Persisting genesis epoch...");
        //         let mut persist_first_epoch_res = self.state.persist_epoch(&epoch).await;
        //         while persist_first_epoch_res.is_err() {
        //             warn!("Failed to persist first epoch, retrying...");
        //             persist_first_epoch_res = self.state.persist_epoch(&epoch).await;
        //         }
        //         epoch_db_guard.stop_and_record();
        //         self.metrics.total_epoch_committed.inc();
        //         info!("Persisted genesis epoch");
        //     } else {
        //         // // NOTE: when the channel is full, epoch_sender_guard will wait until the channel has space.
        //         // self.epoch_indexing_sender.send(epoch).await.map_err(|e| {
        //         //     error!(
        //         //         "Failed to send indexed epoch to epoch commit handler with error {}",
        //         //         e.to_string()
        //         //     );
        //         //     IndexerError::MpscChannelError(e.to_string())
        //         // })?;
        //     }
        // }
        info!(
            checkpoint_seq,
            elapsed, "Checkpoint indexing finished, about to sending to commit handler"
        );
        // NOTE: when the channel is full, checkpoint_sender_guard will wait until the channel has space.
        // Checkpoints are sent sequentially to stick to the order of checkpoint sequence numbers.
        self.indexed_checkpoint_sender
            .send(checkpoint)
            .await
            .tap_ok(|_| info!(checkpoint_seq, "Checkpoint sent to commit handler"))
            .unwrap_or_else(|e| {
                panic!(
                    "checkpoint channel send should not fail, but got error: {:?}",
                    e
                )
            });

        Ok(())
    }
}

struct CheckpointDataObjectStore<'a> {
    objects: &'a [Object],
}

impl<'a> sui_types::storage::ObjectStore for CheckpointDataObjectStore<'a> {
    fn get_object(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::error::SuiError> {
        Ok(self.objects.iter().find(|o| o.id() == *object_id).cloned())
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_types::base_types::VersionNumber,
    ) -> Result<Option<Object>, sui_types::error::SuiError> {
        Ok(self
            .objects
            .iter()
            .find(|o| o.id() == *object_id && o.version() == version)
            .cloned())
    }
}

impl<S> CheckpointHandler<S>
where
    S: IndexerStoreV2 + Clone + Sync + Send + 'static,
{
    // FIXME: This handler is problematic:
    // `get_sui_system_state` always returns the latest state
    async fn index_epoch(
        state: &S,
        data: &CheckpointData,
    ) -> Result<Option<TemporaryEpochStoreV2>, IndexerError> {
        let CheckpointData {
            transactions,
            checkpoint_summary,
            checkpoint_contents: _,
            objects,
        } = data;

        let checkpoint_object_store = CheckpointDataObjectStore { objects };

        // NOTE: Index epoch when object checkpoint index has reached the same checkpoint,
        // because epoch info is based on the latest system state object by the current checkpoint.

        // Genesis epoch
        if *checkpoint_summary.sequence_number() == 0 {
            info!("Processing genesis epoch");
            // very first epoch
            let system_state: SuiSystemStateSummary =
                get_sui_system_state(&checkpoint_object_store)?.into_sui_system_state_summary();
            return Ok(Some(TemporaryEpochStoreV2 {
                last_epoch: None,
                new_epoch: IndexedEpochInfo {
                    epoch: 0,
                    first_checkpoint_id: 0,
                    epoch_start_timestamp: system_state.epoch_start_timestamp_ms,
                    validators: system_state.active_validators,
                    reference_gas_price: system_state.reference_gas_price,
                    protocol_version: system_state.protocol_version,
                    // Below is to be filled by end of epoch
                    epoch_total_transactions: 0,
                    end_of_epoch_info: None,
                    end_of_epoch_data: None,
                },
            }));
        }

        // If not end of epoch, return
        if checkpoint_summary.end_of_epoch_data.is_none() {
            return Ok(None);
        }

        let system_state: SuiSystemStateSummary =
            get_sui_system_state(&checkpoint_object_store)?.into_sui_system_state_summary();

        let epoch_event = transactions
            .iter()
            .flat_map(|(_, _, events)| events.as_ref().map(|e| &e.data))
            .flatten()
            .find(|ev| ev.is_system_epoch_info_event())
            .unwrap_or_else(|| {
                panic!(
                    "Can't find SystemEpochInfoEvent in epoch end checkpoint {}",
                    checkpoint_summary.sequence_number()
                )
            });

        let event = bcs::from_bytes::<SystemEpochInfoEvent>(&epoch_event.contents)?;

        let validators = system_state.active_validators;

        let last_epoch = system_state.epoch - 1;
        let network_tx_count_prev_epoch = state
            .get_network_total_transactions_previous_epoch(last_epoch)
            .await?;

        let last_end_of_epoch_info = EndOfEpochInfo {
            last_checkpoint_id: *checkpoint_summary.sequence_number(),
            epoch_end_timestamp: checkpoint_summary.timestamp_ms,
            protocol_version: event.protocol_version,
            reference_gas_price: event.reference_gas_price,
            total_stake: event.total_stake,
            storage_fund_reinvestment: event.storage_fund_reinvestment,
            storage_charge: event.storage_charge,
            storage_rebate: event.storage_rebate,
            leftover_storage_fund_inflow: event.leftover_storage_fund_inflow,
            stake_subsidy_amount: event.stake_subsidy_amount,
            storage_fund_balance: event.storage_fund_balance,
            total_gas_fees: event.total_gas_fees,
            total_stake_rewards_distributed: event.total_stake_rewards_distributed,
        };
        Ok(Some(TemporaryEpochStoreV2 {
            last_epoch: Some(IndexedEndOfEpochInfo {
                epoch: system_state.epoch - 1,
                end_of_epoch_info: last_end_of_epoch_info,
                end_of_epoch_data: checkpoint_summary
                    .end_of_epoch_data
                    .as_ref()
                    .unwrap()
                    .clone(),
                epoch_total_transactions: checkpoint_summary.network_total_transactions
                    - network_tx_count_prev_epoch,
            }),
            new_epoch: IndexedEpochInfo {
                epoch: system_state.epoch,
                validators,
                first_checkpoint_id: checkpoint_summary.sequence_number + 1,
                epoch_start_timestamp: system_state.epoch_start_timestamp_ms,
                protocol_version: system_state.protocol_version,
                reference_gas_price: system_state.reference_gas_price,
                // Below is to be filled by end of epoch
                end_of_epoch_info: None,
                end_of_epoch_data: None,
                epoch_total_transactions: 0,
            },
        }))
    }

    async fn index_checkpoint_and_epoch(
        state: &S,
        starting_tx_sequence_number: u64,
        data: CheckpointData,
        object_cache: Arc<Mutex<InMemObjectCache>>,
        sui_client: Arc<SuiClient>,
    ) -> Result<(TemporaryCheckpointStoreV2, Option<TemporaryEpochStoreV2>), IndexerError> {
        let (checkpoint, db_transactions, db_events, db_indices) = {
            let CheckpointData {
                transactions,
                checkpoint_summary,
                checkpoint_contents,
                objects,
            } = &data;

            let mut db_transactions = Vec::new();
            let mut db_events = Vec::new();
            let mut db_indices = Vec::new();

            // info!(
            //     "All fetched objects: {:?}",
            //     data.objects
            //         .iter()
            //         .map(|o| (o.id(), o.version().value()))
            //         .collect::<Vec<_>>()
            // );
            // let all_changed_objects_debug = data
            //     .transactions
            //     .iter()
            //     .flat_map(|(_, fx, _)| {
            //         fx.all_changed_objects()
            //             .into_iter()
            //             .map(|(oref, _owner, kind)| (oref, kind))
            //     })
            //     .collect::<Vec<_>>();
            // // info!("All changed objects: {:?}", all_changed_objects_debug);
            // let deleted_objects = data
            //     .transactions
            //     .iter()
            //     .flat_map(|(_, fx, _)| get_deleted_objects(fx))
            //     .collect::<Vec<_>>();
            // info!("All deleted objects: {:?}", deleted_objects);

            for (idx, (tx, fx, events)) in transactions.into_iter().enumerate() {
                let tx_sequence_number = starting_tx_sequence_number + idx as u64;
                let tx_digest = tx.digest();
                let tx = tx.transaction_data();
                let events = events
                    .as_ref()
                    .map(|events| events.data.clone())
                    .unwrap_or_default();

                let transaction_kind = if tx.is_system_tx() {
                    TransactionKind::SystemTransaction
                } else {
                    TransactionKind::ProgrammableTransaction
                };

                db_events.extend(events.iter().enumerate().map(|(idx, event)| {
                    IndexedEvent::from_event(
                        tx_sequence_number,
                        idx as u64,
                        *tx_digest,
                        event,
                        checkpoint_summary.timestamp_ms,
                    )
                }));

                let (balance_change, object_changes) =
                    TxChangesProcessor::new(state, &objects, object_cache.clone(), sui_client.clone())
                        .get_changes(tx, &fx, &tx_digest)
                        .await?;

                let db_txn = IndexedTransaction {
                    tx_sequence_number,
                    tx_digest: *tx_digest,
                    checkpoint_sequence_number: *checkpoint_summary.sequence_number(),
                    timestamp_ms: checkpoint_summary.timestamp_ms,
                    transaction: tx.clone(),
                    effects: fx.clone(),
                    object_changes,
                    balance_change,
                    events,
                    transaction_kind,
                    successful_tx_num: if fx.status().is_ok() {
                        tx.kind().num_commands() as u64
                    } else {
                        0
                    },
                };

                db_transactions.push(db_txn);

                // Input Objects
                let input_objects = tx
                    .input_objects()
                    .expect("committed txns have been validated")
                    .into_iter()
                    .map(|obj_kind| obj_kind.object_id())
                    .collect::<Vec<_>>();

                // Changed Objects
                let changed_objects = fx
                    .all_changed_objects()
                    .into_iter()
                    .map(|(object_ref, _owner, _write_kind)| object_ref.0)
                    .collect::<Vec<_>>();

                // Senders
                let senders = vec![tx.sender()];

                // Recipients
                let recipients = fx
                    .all_changed_objects()
                    .into_iter()
                    .filter_map(|(_object_ref, owner, _write_kind)| match owner {
                        Owner::AddressOwner(address) => Some(address),
                        _ => None,
                    })
                    .unique()
                    .collect::<Vec<_>>();

                // Move Calls
                let move_calls = tx
                    .move_calls()
                    .iter()
                    .map(|(p, m, f)| (*p.clone(), m.to_string(), f.to_string()))
                    .collect();

                db_indices.push(TxIndex {
                    tx_sequence_number,
                    transaction_digest: *tx_digest,
                    input_objects,
                    changed_objects,
                    senders,
                    recipients,
                    move_calls,
                });
            }
            let successful_tx_num: u64 = db_transactions.iter().map(|t| t.successful_tx_num).sum();
            (
                IndexedCheckpoint::from_sui_checkpoint(
                    checkpoint_summary,
                    checkpoint_contents,
                    successful_tx_num as usize,
                ),
                db_transactions,
                db_events,
                db_indices,
            )
        };

        let epoch_index = Self::index_epoch(state, &data).await?;

        // Index Objects

        let (object_changes, packages) = Self::index_checkpoint(state, data, object_cache).await;

        Ok((
            TemporaryCheckpointStoreV2 {
                checkpoint,
                transactions: db_transactions,
                events: db_events,
                tx_indices: db_indices,
                object_changes,
                packages,
            },
            epoch_index,
        ))
    }

    async fn index_checkpoint(
        state: &S,
        // packages_handler: S,
        data: CheckpointData,
        object_cache: Arc<Mutex<InMemObjectCache>>,
    ) -> (TransactionObjectChangesV2, Vec<IndexedPackage>) {
        info!(
            checkpoint_seq = data.checkpoint_summary.sequence_number,
            "Indexing checkpoint"
        );
        // // Index packages
        let packages = Self::index_packages(&data);
        // Index objects
        let epoch = data.checkpoint_summary.epoch();
        let checkpoint_seq = *data.checkpoint_summary.sequence_number();

        let deleted_objects = data
            .transactions
            .iter()
            .flat_map(|(_, fx, _)| get_deleted_objects(fx))
            .collect::<Vec<_>>();

        // info!("All fetched objects: {:?}", data.objects.iter().map(|o| (o.id(), o.version().value())).collect::<Vec<_>>());
        // let all_changed_objects_debug = data.transactions.iter().flat_map(|(_, fx, _)| {
        //     fx.all_changed_objects().into_iter().map(|(oref, _owner, kind)| (oref, kind))
        // }).collect::<Vec<_>>();
        // info!("All changed objects: {:?}", all_changed_objects_debug);
        // info!("All deleted objects: {:?}", deleted_objects);
        let deleted_object_ids = deleted_objects
            .iter()
            .map(|o| (o.0, o.1))
            .collect::<HashSet<_>>();

        let (objects, discarded_versions) = get_latest_objects(data.objects);

        let module_resolver = InterimModuleResolver::new(state.module_cache(), object_cache, &packages);
        let changed_objects = data
            .transactions
            .iter()
            .flat_map(|(tx, fx, _)| {
                let changed_objects = fx
                    .all_changed_objects()
                    .into_iter()
                    .filter_map(|(oref, _owner, kind)| {
                        if discarded_versions.contains(&(oref.0, oref.1))
                            || deleted_object_ids.contains(&(oref.0, oref.1))
                        {
                            return None;
                        }
                        let object = objects.get(&(oref.0)).unwrap_or_else(|| {
                            panic!(
                                "object {:?} not found in CheckpointData (tx_digest: {})",
                                oref.0,
                                tx.digest()
                            )
                        });
                        assert_eq!(oref.1, object.version());
                        // let module_cache = state.module_cache();
                        let df_info =
                            try_create_dynamic_field_info(object, &objects, &module_resolver)
                                .expect("failed to create dynamic field info");
                        Some(IndexedObject::from_object(
                            checkpoint_seq,
                            object.clone(),
                            df_info,
                        ))
                    })
                    .collect::<Vec<_>>();
                changed_objects
            })
            .collect();

        (
            TransactionObjectChangesV2 {
                changed_objects,
                deleted_objects,
            },
            packages,
        )
    }

    fn index_packages(checkpoint_data: &CheckpointData) -> Vec<IndexedPackage> {
        checkpoint_data
            .objects
            .iter()
            .filter_map(|o| {
                if let sui_types::object::Data::Package(p) = &o.data {
                    Some(IndexedPackage {
                        package_id: o.id(),
                        move_package: p.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

pub async fn start_tx_checkpoint_commit_task<S>(
    state: S,
    metrics: IndexerMetrics,
    config: IndexerConfig,
    tx_indexing_receiver: mysten_metrics::metered_channel::Receiver<TemporaryCheckpointStoreV2>,
) where
    S: IndexerStoreV2 + Clone + Sync + Send + 'static,
{
    use futures::StreamExt;

    info!("Indexer checkpoint commit task started...");
    let checkpoint_commit_batch_size = std::env::var("CHECKPOINT_COMMIT_BATCH_SIZE")
        .unwrap_or(5.to_string())
        .parse::<usize>()
        .unwrap();
    info!("Using checkpoint commit batch size {checkpoint_commit_batch_size}");

    let mut stream = mysten_metrics::metered_channel::ReceiverStream::new(tx_indexing_receiver)
        .ready_chunks(checkpoint_commit_batch_size);

    while let Some(indexed_checkpoint_batch) = stream.next().await {
        let mut checkpoint_batch = vec![];
        let mut tx_batch = vec![];
        let mut events_batch = vec![];
        let mut tx_indices_batch = vec![];
        let mut object_changes_batch = vec![];
        let mut packages_batch = vec![];

        if config.skip_db_commit {
            info!(
                "[Checkpoint/Tx] Downloaded and indexed checkpoint {:?} - {:?} successfully, skipping DB commit...",
                indexed_checkpoint_batch.first().map(|c| c.checkpoint.sequence_number),
                indexed_checkpoint_batch.last().map(|c| c.checkpoint.sequence_number),
            );
            continue;
        }

        // FIXME rewrite this
        for indexed_checkpoint in indexed_checkpoint_batch {
            // Write checkpoint to DB
            let TemporaryCheckpointStoreV2 {
                checkpoint,
                transactions,
                events,
                tx_indices,
                object_changes,
                packages,
            } = indexed_checkpoint;
            checkpoint_batch.push(checkpoint);
            tx_batch.push(transactions);
            events_batch.push(events);
            tx_indices_batch.push(tx_indices);
            object_changes_batch.push(object_changes);
            packages_batch.push(packages);
        }

        let first_checkpoint_seq = checkpoint_batch.first().as_ref().unwrap().sequence_number;
        let last_checkpoint_seq = checkpoint_batch.last().as_ref().unwrap().sequence_number;
        let checkpoint_num = checkpoint_batch.len();
        let tx_count = tx_batch.len();

        let guard = metrics.checkpoint_db_commit_latency.start_timer();
        let tx_batch = tx_batch.into_iter().flatten().collect::<Vec<_>>();
        let tx_indices_batch = tx_indices_batch.into_iter().flatten().collect::<Vec<_>>();
        let events_batch = events_batch.into_iter().flatten().collect::<Vec<_>>();
        let packages_batch = packages_batch.into_iter().flatten().collect::<Vec<_>>();

        futures::future::join_all(vec![
            state.persist_transactions(tx_batch),
            state.persist_tx_indices(tx_indices_batch),
            state.persist_events(events_batch),
            state.persist_object_changes(object_changes_batch),
            state.persist_packages(packages_batch),
        ])
        .await
        .into_iter()
        .map(|res| {
            if res.is_err() {
                error!("Failed to persist data with error: {:?}", res);
            }
            res
        })
        .collect::<IndexerResult<Vec<_>>>()
        .expect("Persisting data into DB should not fail.");

        state
            .persist_checkpoints(
                checkpoint_batch,
                // &tx_batch,
                // metrics.total_transaction_chunk_committed.clone(),
            )
            .await
            .tap_err(|e| {
                error!(
                    "Failed to persist checkpoint data with error: {}",
                    e.to_string()
                );
            })
            .expect("Persisting data into DB should not fail.");
        let elapsed = guard.stop_and_record();

        // unwrap: batch must not be empty at this point
        metrics
            .latest_tx_checkpoint_sequence_number
            .set(last_checkpoint_seq as i64);

        metrics
            .total_tx_checkpoint_committed
            .inc_by(checkpoint_num as u64);
        metrics.total_transaction_committed.inc_by(tx_count as u64);
        info!(
            elapsed,
            "Checkpoint {}-{} committed with {} transactions.",
            first_checkpoint_seq,
            last_checkpoint_seq,
            tx_count,
        );
        metrics
            .transaction_per_checkpoint
            .observe(tx_count as f64 / (last_checkpoint_seq - first_checkpoint_seq + 1) as f64);
        // 1000.0 is not necessarily the batch size, it's to roughly map average tx commit latency to [0.1, 1] seconds,
        // which is well covered by DB_COMMIT_LATENCY_SEC_BUCKETS.
        metrics
            .thousand_transaction_avg_db_commit_latency
            .observe(elapsed * 1000.0 / tx_count as f64);
    }
}

// FIXME clean up by checkpoint
pub struct InMemObjectCache {
    id_map: HashMap<ObjectID, Arc<Object>>,
    seq_map: HashMap<(ObjectID, SequenceNumber), Arc<Object>>,
    packages: HashMap<(ObjectID, String), Arc<CompiledModule>>,
}

impl InMemObjectCache {
    pub fn new() -> Self {
        Self {
            id_map: HashMap::new(),
            seq_map: HashMap::new(),
            packages: HashMap::new(),
        }
    }

    pub fn insert_object(&mut self, object: Object) {
        let obj = Arc::new(object);
        self.id_map.insert(obj.id(), obj.clone());
        self.seq_map.insert((obj.id(), obj.version()), obj);
    }

    pub fn insert_packages(&mut self, new_packages: &Vec<IndexedPackage>) {
        let new_packages = new_packages
            .iter()
            .flat_map(|p| {
                p.move_package
                    .serialized_module_map()
                    .iter()
                    .map(|(module_name, bytes)| {
                        tracing::info!("insert packages, package id: {:?}, module_name: {:?}", p.package_id, module_name);
                        let module = CompiledModule::deserialize_with_defaults(&bytes).unwrap();
                        ((p.package_id.clone(), module_name.clone()), Arc::new(module))
                    })
            })
            .collect::<HashMap<_, _>>();
        self.packages.extend(new_packages);
    }

    pub fn get(&self, id: &ObjectID, version: Option<&SequenceNumber>) -> Option<&Object> {
        if let Some(version) = version {
            self.seq_map.get(&(*id, *version)).map(|o: &Arc<Object>| o.as_ref())
        } else {
            self.id_map.get(id).map(|o| o.as_ref())
        }
    }

    pub fn get_module_by_id(&self, id: &ModuleId) -> Option<Arc<CompiledModule>> {
        let package_id = ObjectID::from(id.address().clone());
        let name = id.name().to_string();
        self.packages.get(&(package_id, name)).cloned()
    }
}

pub struct TxChangesProcessor<'a, S> {
    state: &'a S,
    // FIXME: why do we still need updated_coin_objects if we have all_objects?
    // updated_coin_objects: HashMap<(ObjectID, SequenceNumber), Object>,
    // TODO: Store only the reference
    // all_objects: HashMap<(ObjectID, SequenceNumber), Object>,
    object_cache: Arc<Mutex<InMemObjectCache>>,
    sui_client: Arc<SuiClient>,
}

impl<'a, S> TxChangesProcessor<'a, S>
where
    S: IndexerStoreV2 + Clone + Sync + Send,
{
    pub fn new(state: &'a S, objects: &[Object], object_cache: Arc<Mutex<InMemObjectCache>>, sui_client: Arc<SuiClient>) -> Self {
        // let mut updated_coin_objects = HashMap::new();
        // let mut all_objects: HashMap<(ObjectID, SequenceNumber), Object> = HashMap::new();
        for obj in objects {
            object_cache.lock().unwrap().insert_object(obj.clone());
            // tracing::error!(
            //     "Insert Object {:?} with version {:?}",
            //     obj.id(),
            //     obj.version()
            // );
        }
        Self {
            state,
            // updated_coin_objects,
            object_cache,
            sui_client,
            // all_objects
        }
    }

    async fn get_changes(
        &self,
        tx: &TransactionData,
        effects: &TransactionEffects,
        tx_digest: &TransactionDigest,
    ) -> IndexerResult<(
        Vec<sui_json_rpc_types::BalanceChange>,
        Vec<sui_json_rpc_types::ObjectChange>,
    )> {
        // info!(
        //     "TxChangesProcessor::get_changes, tx_digest: {:?}",
        //     tx_digest
        // );
        let object_change: Vec<sui_json_rpc_types::ObjectChange> = get_object_changes(
            self,
            tx.sender(),
            effects.modified_at_versions(),
            effects.all_changed_objects(),
            effects.all_removed_objects(),
        )
        .await?;
        let balance_change = get_balance_changes_from_effect(
            self,
            &effects,
            tx.input_objects().unwrap_or_else(|e| {
                panic!(
                    "Checkpointed tx {:?} has inavlid input objects: {e}",
                    tx_digest,
                )
            }),
            None,
        )
        .await?;
        Ok((balance_change, object_change))
    }
}

// Note: the implementation of `ObjectProvider` for `TxChangesProcessor`
// is NOT trivial. It needs to be a ObjectProvider to do
// `try_create_dynamic_field_info`. So the logic below is tailored towards that.

// FIXME: can we not panic here but in the callsite of these functions?
#[async_trait]
impl<'a, S> ObjectProvider for TxChangesProcessor<'a, S>
where
    S: IndexerStoreV2 + Clone + Sync + Send,
{
    type Error = IndexerError;

    async fn get_object(
        &self,
        id: &ObjectID,
        version: &SequenceNumber,
    ) -> Result<Object, Self::Error> {
        // tracing::error!(
        //     "TxChangesProcessor::get_object, object id {:?}, v: {:?}",
        //     id,
        //     version
        // );
        let object = self.object_cache.lock().unwrap().get(id, Some(version)).as_ref().map(|o| o.clone().clone());
        if let Some(o) = object {
            return Ok(o);
        }

        if let Some(object) = self.state.get_object(*id, Some(*version)).await? {
            return Ok(object)
        }

        // Last resort - read the version from remote. Here's an edge case why this may be needed:
        // Say object O is at version V1 at Checkpoint C1, and then updated to V2 at Checkpoint C2.
        // When we process C2, we calculate the Object/BalanceChange and what not, all go well.
        // But the DB commits takes two steps, 1. commit txes, objects, etc and 2. commit checkpoints.
        // If the system crashed between these two steps, when it restarts, only V2 can be found in DB.
        // It needs to reprocess C2 because checkpoint data is not committed yet. Now it will find
        // difficulty to get V1.
        // If we always commits everything in one DB transactions, then this is a non-issue. However:
        // 1. this is a big commitment that comes with performance trade-offs
        // 2. perhaps one day we will use a system that has no transaction support.
        let object = self.sui_client.read_api().try_get_parsed_past_object(
            *id,
            *version,
            SuiObjectDataOptions::bcs_lossless(),
        ).await.map_err(|e|IndexerError::FullNodeReadingError(e.to_string()))?
        .into_object().map_err(|e|IndexerError::DataTransformationError(e.to_string()))?
        .try_into().map_err(|e: anyhow::Error| IndexerError::DataTransformationError(e.to_string()))?;

        Ok(object)
    }

    async fn find_object_lt_or_eq_version(
        &self,
        id: &ObjectID,
        version: &SequenceNumber,
    ) -> Result<Option<Object>, Self::Error> {
        tracing::error!(
            "TxChangesProcessor::find_object_lt_or_eq_version, object id {:?}, v: {:?}",
            id,
            version
        );
        // First look up the exact version in object_cache.
        // If the exact version is generated in the current checkpoint, we should find it here.
        let object = self.object_cache.lock().unwrap().get(id, Some(version)).as_ref().map(|o| o.clone().clone());
        if let Some(o) = object {
            return Ok(Some(o));
        }

        // // Second look up the latest version in object_cache, if it happens to be there
        // // Because the way object_cache is updated, the object there must be the latest version
        // // that it knows. Put it in another way, the latest object version in object_cache
        // // must be newer if not equal to the version in database.
        let object = self.object_cache.lock().unwrap().get(id, None).as_ref().map(|o| o.clone().clone());
        if let Some(o) = object {
            // If the object is updated multiple times in the same checkpoint,
            // we may not find the version that lt_or_eq to the given version.
            // In this case, we default 
            if o.version() <= *version {
                return Ok(Some(o));
            }
        }

        // Second, look up the object with the latest version and make sure the version is lt_or_eq
        match self.state.get_object(*id, None).await? {
            None => {
                panic!("Object {} is not found", id);
            }
            Some(object) => {
                assert!(object.version() <= *version);
                Ok(Some(object))
            }
        }
    }
}

pub fn get_deleted_objects(effects: &TransactionEffects) -> Vec<ObjectRef> {
    let deleted = effects.deleted().into_iter();
    let wrapped = effects.wrapped().into_iter();
    let unwrapped_then_deleted = effects.unwrapped_then_deleted().into_iter();
    deleted
        .chain(wrapped)
        .chain(unwrapped_then_deleted)
        .collect::<Vec<_>>()
}

pub fn get_latest_objects(
    objects: Vec<Object>,
) -> (
    HashMap<ObjectID, Object>,
    HashSet<(ObjectID, SequenceNumber)>,
) {
    let mut latest_objects = HashMap::new();
    let mut discarded_versions = HashSet::new();
    for object in objects {
        match latest_objects.entry(object.id().clone()) {
            Entry::Vacant(e) => {
                e.insert(object);
            }
            Entry::Occupied(mut e) => {
                if object.version() > e.get().version() {
                    discarded_versions.insert((e.get().id().clone(), e.get().version()));
                    e.insert(object);
                }
            }
        }
    }
    (latest_objects, discarded_versions)
}

fn try_create_dynamic_field_info(
    o: &Object,
    written: &HashMap<ObjectID, Object>,
    resolver: &impl GetModule,
) -> IndexerResult<Option<DynamicFieldInfo>> {
    // Skip if not a move object
    let Some(move_object) = o.data.try_as_move().cloned() else {
        return Ok(None);
    };

    if !move_object.type_().is_dynamic_field() {
        return Ok(None);
    }

    // info!(
    //     "@@@@@@@@@@@@@@ try_create_dynamic_field_info obj id: {:?}, {}",
    //     o.id(),
    //     o.version()
    // );
    let move_struct =
        move_object.to_move_struct_with_resolver(ObjectFormatOptions::default(), resolver)?;

    let (name_value, type_, object_id) =
        DynamicFieldInfo::parse_move_object(&move_struct).tap_err(|e| warn!("{e}"))?;

    let name_type = move_object.type_().try_extract_field_name(&type_)?;

    let bcs_name = bcs::to_bytes(&name_value.clone().undecorate()).map_err(|e| {
        IndexerError::SerdeError(format!(
            "Failed to serialize dynamic field name {:?}: {e}",
            name_value
        ))
    })?;

    let name = DynamicFieldName {
        type_: name_type,
        value: SuiMoveValue::from(name_value).to_json_value(),
    };
    Ok(Some(match type_ {
        DynamicFieldType::DynamicObject => {
            let object = written
                .get(&object_id)
                .ok_or(IndexerError::UncategorizedError(anyhow::anyhow!(
                    "Failed to find object_id {:?} when trying to create dynamic field info",
                    object_id
                )))?;
            let version = object.version();
            let digest = object.digest();
            let object_type = object.data.type_().unwrap().clone();
            DynamicFieldInfo {
                name,
                bcs_name,
                type_,
                object_type: object_type.to_string(),
                object_id,
                version,
                digest,
            }
        }
        DynamicFieldType::DynamicField => DynamicFieldInfo {
            name,
            bcs_name,
            type_,
            object_type: move_object.into_type().into_type_params()[1].to_string(),
            object_id: o.id(),
            version: o.version(),
            digest: o.digest(),
        },
    }))
}
