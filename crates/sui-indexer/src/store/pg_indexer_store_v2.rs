// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use core::result::Result::Ok;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use cached::proc_macro::once;
use diesel::dsl::{count, max};
use diesel::pg::PgConnection;
use diesel::sql_types::{BigInt, VarChar};
use diesel::upsert::excluded;
use diesel::ExpressionMethods;
use diesel::{OptionalExtension, QueryableByName};
use diesel::{QueryDsl, RunQueryDsl};
use fastcrypto::hash::Digest;
use fastcrypto::traits::ToFromBytes;
use move_bytecode_utils::module_cache::SyncModuleCache;
use move_core_types::identifier::Identifier;
use mysten_metrics::monitored_scope;
use prometheus::{Histogram, IntCounter};
use tracing::info;

use sui_json_rpc_types::SuiTransactionBlockResponse;
use sui_json_rpc_types::{CheckpointId, EpochInfo, EventFilter, EventPage, SuiEvent};
use sui_types::base_types::{ObjectID, SequenceNumber, SuiAddress};
use sui_types::committee::{EpochId, ProtocolVersion};
use sui_types::crypto::AuthorityPublicKeyBytes;
use sui_types::digests::CheckpointDigest;
use sui_types::digests::TransactionDigest;
use sui_types::event::EventID;
use sui_types::messages_checkpoint::{CheckpointSequenceNumber, EndOfEpochData};
use sui_types::object::{Object, ObjectRead};

use crate::errors::{Context, IndexerError};
use crate::metrics::IndexerMetrics;

use crate::models_v2::checkpoints::StoredCheckpoint;
use crate::models_v2::epoch::{StoredEndOfEpochInfo, StoredEpochInfo};
use crate::models_v2::events::StoredEvent;
use crate::models_v2::objects::{StoredDeletedObject, StoredObject};
use crate::models_v2::packages::StoredPackage;
use crate::models_v2::transactions::StoredTransaction;
use crate::models_v2::tx_indices::StoredTxIndex;
use crate::schema_v2::{checkpoints, epochs, events, objects, packages, transactions, tx_indices};
use crate::store::diesel_marco::{
    read_only_blocking, transactional_blocking, transactional_blocking_with_retry,
};
use crate::store::module_resolver_v2::IndexerModuleResolverV2;
use crate::types_v2::{
    IndexedCheckpoint, IndexedEvent, IndexedObject, IndexedPackage, IndexedTransaction, TxIndex,
};
use crate::PgConnectionPool;

use super::{IndexerStoreV2, TemporaryEpochStoreV2, TransactionObjectChangesV2};

const PG_COMMIT_CHUNK_SIZE: usize = 1000;

#[derive(Clone)]
pub struct PgIndexerStoreV2 {
    blocking_cp: PgConnectionPool,
    module_cache: Arc<SyncModuleCache<IndexerModuleResolverV2>>,
    metrics: IndexerMetrics,
}

impl PgIndexerStoreV2 {
    pub fn new(blocking_cp: PgConnectionPool, metrics: IndexerMetrics) -> Self {
        let module_cache: Arc<SyncModuleCache<IndexerModuleResolverV2>> = Arc::new(
            SyncModuleCache::new(IndexerModuleResolverV2::new(blocking_cp.clone())),
        );
        Self {
            blocking_cp,
            module_cache,
            metrics,
        }
    }

    fn get_latest_tx_checkpoint_sequence_number(&self) -> Result<Option<u64>, IndexerError> {
        read_only_blocking!(&self.blocking_cp, |conn| {
            checkpoints::dsl::checkpoints
                .select(max(checkpoints::sequence_number))
                .first::<Option<i64>>(conn)
                .map(|v| v.map(|v| v as u64))
        })
        .context("Failed reading latest checkpoint sequence number from PostgresDB")
    }

    fn get_checkpoint_ending_tx_sequence_number(
        &self,
        seq_num: CheckpointSequenceNumber,
    ) -> Result<Option<u64>, IndexerError> {
        read_only_blocking!(&self.blocking_cp, |conn| {
            checkpoints::dsl::checkpoints
                .select(checkpoints::network_total_transactions)
                .filter(checkpoints::sequence_number.eq(seq_num as i64))
                .first::<i64>(conn)
                .optional()
                .map(|v| v.map(|v| v as u64))
        })
        .context("Failed reading checkpoint end tx sequence number from PostgresDB")
    }

    fn get_checkpoint_sequence_number(
        &self,
        digest: CheckpointDigest,
    ) -> Result<CheckpointSequenceNumber, IndexerError> {
        Ok(
            read_only_blocking!(&self.blocking_cp, |conn| checkpoints::dsl::checkpoints
                .select(checkpoints::sequence_number)
                .filter(checkpoints::checkpoint_digest.eq(digest.into_inner().to_vec()))
                .first::<i64>(conn))
            .context("Failed reading checkpoint seq number from PostgresDB")? as u64,
        )
    }

    fn get_object(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<Option<Object>, IndexerError> {
        // tracing::error!("get_object: {:?} {:?}", object_id, version);
        // TODO 1: if not found, read deleted_object
        // TOOD 2: read remote object_history kv store
        read_only_blocking!(&self.blocking_cp, |conn| {
            let query =
                objects::dsl::objects.filter(objects::dsl::object_id.eq(object_id.to_vec()));
            let boxed_query = if let Some(version) = version {
                query
                    .filter(objects::dsl::object_version.eq(version.value() as i64))
                    .into_boxed()
            } else {
                query.into_boxed()
            };
            match boxed_query.first::<StoredObject>(conn).optional()? {
                None => Ok(None),
                Some(obj) => Object::try_from(obj).map(Some),
            }
        })
        .context("Failed to read object from PostgresDB")
    }

    fn get_object_read(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<ObjectRead, IndexerError> {
        // TODO 1: if not found, read deleted_object
        // TOOD 2: read remote object_history kv store
        read_only_blocking!(&self.blocking_cp, |conn| {
            let query =
                objects::dsl::objects.filter(objects::dsl::object_id.eq(object_id.to_vec()));
            let boxed_query = if let Some(version) = version {
                query
                    .filter(objects::dsl::object_version.eq(version.value() as i64))
                    .into_boxed()
            } else {
                query.into_boxed()
            };
            match boxed_query.first::<StoredObject>(conn).optional()? {
                None => Ok(ObjectRead::NotExists(object_id)),
                Some(obj) => obj.try_into_object_read(self.module_cache.as_ref()),
            }
        })
        .context("Failed to read object from PostgresDB")
    }

    fn persist_checkpoints(&self, checkpoints: Vec<IndexedCheckpoint>) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_checkpoints");
        let checkpoints = checkpoints
            .iter()
            .map(|ckp| StoredCheckpoint::from(ckp))
            .collect::<Vec<_>>();
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                for checkpoint_chunk in checkpoints.chunks(PG_COMMIT_CHUNK_SIZE) {
                    diesel::insert_into(checkpoints::table)
                        .values(checkpoint_chunk)
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .map_err(IndexerError::from)
                        .context("Failed to write checkpoints to PostgresDB")?;
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )
    }

    fn persist_transactions(
        &self,
        transactions: Vec<IndexedTransaction>,
    ) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_transactions");
        let transactions = transactions
            .iter()
            .map(|tx| StoredTransaction::from(tx))
            .collect::<Vec<_>>();
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                for transaction_chunk in transactions.chunks(PG_COMMIT_CHUNK_SIZE) {
                    diesel::insert_into(transactions::table)
                        .values(transaction_chunk)
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .map_err(IndexerError::from)
                        .context("Failed to write transactions to PostgresDB")?;
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )
    }
    fn persist_object_changes(
        &self,
        tx_object_changes: Vec<TransactionObjectChangesV2>,
        // object_mutation_latency: Histogram,
        // object_deletion_latency: Histogram,
        // object_commit_chunk_counter: IntCounter,
    ) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_object_changes");
        let (mutated_objects, deleted_objects) = get_objects_to_commit(tx_object_changes);
        let mutated_objects = mutated_objects
            .into_iter()
            .map(StoredObject::from)
            .collect::<Vec<_>>();
        // let deleted_objects = deleted_objects.into_iter().map(|id| StoredDeletedObject{object_id: id.to_vec()}).collect::<Vec<_>>();
        transactional_blocking_with_retry!(&self.blocking_cp, |conn| {
            for mutated_object_change_chunk in mutated_objects.chunks(PG_COMMIT_CHUNK_SIZE) {
                diesel::insert_into(objects::table)
                    .values(mutated_object_change_chunk)
                    .on_conflict(objects::object_id)
                    .do_update()
                    // .set(objects::all_columns.eq(excluded(objects::all_columns)))
                    .set((
                        objects::object_id.eq(excluded(objects::object_id)),
                        objects::object_version.eq(excluded(objects::object_version)),
                        objects::object_digest.eq(excluded(objects::object_digest)),
                        objects::checkpoint_sequence_number
                            .eq(excluded(objects::checkpoint_sequence_number)),
                        objects::owner_type.eq(excluded(objects::owner_type)),
                        objects::owner_id.eq(excluded(objects::owner_id)),
                        objects::serialized_object.eq(excluded(objects::serialized_object)),
                        objects::coin_type.eq(excluded(objects::coin_type)),
                        objects::coin_balance.eq(excluded(objects::coin_balance)),
                        objects::df_kind.eq(excluded(objects::df_kind)),
                        objects::df_name.eq(excluded(objects::df_name)),
                        objects::df_object_type.eq(excluded(objects::df_object_type)),
                        objects::df_object_id.eq(excluded(objects::df_object_id)),
                    ))
                    .execute(conn)
                    .map_err(IndexerError::from)
                    .context("Failed to write object mutation to PostgresDB")?;
            }
            // TODO: chunk deletion?
            diesel::delete(
                objects::table.filter(
                    objects::object_id.eq_any(
                        deleted_objects
                            .iter()
                            .map(|o| o.to_vec())
                            .collect::<Vec<_>>(),
                    ),
                ),
            )
            .execute(conn)
            .map_err(IndexerError::from)
            .context("Failed to write object deletion to PostgresDB")
            // persist_object_mutations(
            //     conn,
            //     mutated_objects,
            //     object_mutation_latency,
            //     object_commit_chunk_counter.clone(),
            // )?;
            // Ok::<(), IndexerError>(())
        }, Duration::from_secs(60))?;

        // FIXME add deleted objects to deleted table

        // commit object deletions after mutations b/c objects cannot be mutated after deletion,
        // otherwise object mutations might override object deletions.
        // transactional_blocking!(&self.blocking_cp, |conn| {
        //     persist_object_deletions(
        //         conn,
        //         deleted_objects,
        //         object_deletion_latency,
        //         object_commit_chunk_counter,
        //     )?;
        //     Ok::<(), IndexerError>(())
        // })?;
        Ok(())
    }

    fn persist_events(&self, events: Vec<IndexedEvent>) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_events");
        let events = events
            .into_iter()
            .map(StoredEvent::from)
            .collect::<Vec<_>>();
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                for event_chunk in events.chunks(PG_COMMIT_CHUNK_SIZE) {
                    diesel::insert_into(events::table)
                        .values(event_chunk)
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .map_err(IndexerError::from)
                        .context("Failed to write events to PostgresDB")?;
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )
    }

    fn persist_packages(&self, packages: Vec<IndexedPackage>) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_packages");
        let packages = packages
            .into_iter()
            .map(StoredPackage::from)
            .collect::<Vec<_>>();
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                for packages_chunk in packages.chunks(PG_COMMIT_CHUNK_SIZE) {
                    diesel::insert_into(packages::table)
                        .values(packages_chunk)
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .map_err(IndexerError::from)
                        .context("Failed to write packages to PostgresDB")?;
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )
    }

    fn persist_tx_indices(&self, indices: Vec<TxIndex>) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_tx_indices");
        let indices = indices
            .into_iter()
            .map(StoredTxIndex::from)
            .collect::<Vec<_>>();
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                for indices_chunk in indices.chunks(PG_COMMIT_CHUNK_SIZE) {
                    diesel::insert_into(tx_indices::table)
                        .values(indices_chunk)
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .map_err(IndexerError::from)
                        .context("Failed to write tx_indices to PostgresDB")?;
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )
    }

    fn get_network_total_transactions_previous_epoch(
        &self,
        epoch: u64,
    ) -> Result<u64, IndexerError> {
        read_only_blocking!(&self.blocking_cp, |conn| {
            checkpoints::table
                .filter(checkpoints::epoch.eq(epoch as i64 - 1))
                .select(max(checkpoints::network_total_transactions))
                .first::<Option<i64>>(conn)
                .map(|o| o.unwrap_or(0))
        })
        .context("Failed to count network transactions in previous epoch")
        .map(|v| v as u64)
    }

    fn persist_epoch(&self, data: &TemporaryEpochStoreV2) -> Result<(), IndexerError> {
        let _scope = monitored_scope("pg_indexer_store_v2::persist_epoch");
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                if let Some(last_epoch) = &data.last_epoch {
                    let epoch_id = last_epoch.epoch;
                    info!("Updating epoch end data for epoch {}", epoch_id);
                    let last_epoch = StoredEndOfEpochInfo::from(last_epoch);
                    diesel::insert_into(epochs::table)
                        .values(last_epoch)
                        .on_conflict(epochs::epoch)
                        .do_update()
                        .set((
                            epochs::epoch_total_transactions
                                .eq(excluded(epochs::epoch_total_transactions)),
                            epochs::end_of_epoch_info.eq(excluded(epochs::end_of_epoch_info)),
                            epochs::end_of_epoch_data.eq(excluded(epochs::end_of_epoch_data)),
                        ))
                        .execute(conn)?;
                    info!("Updated epoch end data for epoch {}", epoch_id);
                }
                Ok::<(), IndexerError>(())
            },
            Duration::from_secs(60)
        )?;
        info!("Persisting initial state of epoch {}", data.new_epoch.epoch);
        transactional_blocking_with_retry!(
            &self.blocking_cp,
            |conn| {
                let new_epoch = StoredEpochInfo::from(&data.new_epoch);
                diesel::insert_into(epochs::table)
                    .values(new_epoch)
                    .on_conflict_do_nothing()
                    .execute(conn)
            },
            Duration::from_secs(60)
        )?;
        info!("Persisted initial state of epoch {}", data.new_epoch.epoch);
        Ok(())
    }

    fn get_epochs(
        &self,
        cursor: Option<EpochId>,
        limit: usize,
        descending_order: Option<bool>,
    ) -> Result<Vec<EpochInfo>, IndexerError> {
        unimplemented!()
    }

    fn get_current_epoch(&self) -> Result<EpochInfo, IndexerError> {
        unimplemented!()
    }

    async fn spawn_blocking<F, R>(&self, f: F) -> Result<R, IndexerError>
    where
        F: FnOnce(Self) -> Result<R, IndexerError> + Send + 'static,
        R: Send + 'static,
    {
        let this = self.clone();
        tokio::task::spawn_blocking(move || f(this))
            .await
            .map_err(Into::into)
            .and_then(std::convert::identity)
    }
}

#[async_trait]
impl IndexerStoreV2 for PgIndexerStoreV2 {
    type ModuleCache = SyncModuleCache<IndexerModuleResolverV2>;

    async fn get_latest_tx_checkpoint_sequence_number(&self) -> Result<Option<u64>, IndexerError> {
        self.spawn_blocking(|this| this.get_latest_tx_checkpoint_sequence_number())
            .await
    }

    async fn get_checkpoint_ending_tx_sequence_number(
        &self,
        seq_num: CheckpointSequenceNumber,
    ) -> Result<Option<u64>, IndexerError> {
        self.spawn_blocking(move |this| this.get_checkpoint_ending_tx_sequence_number(seq_num))
            .await
    }

    async fn get_checkpoint(
        &self,
        id: CheckpointId,
    ) -> Result<sui_json_rpc_types::Checkpoint, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_checkpoint(id))
        //     .await
    }

    async fn get_checkpoints(
        &self,
        cursor: Option<CheckpointId>,
        limit: usize,
    ) -> Result<Vec<sui_json_rpc_types::Checkpoint>, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_checkpoints(cursor, limit))
        //     .await
    }

    async fn get_checkpoint_sequence_number(
        &self,
        digest: CheckpointDigest,
    ) -> Result<CheckpointSequenceNumber, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_checkpoint_sequence_number(digest))
        //     .await
    }

    async fn get_event(&self, id: EventID) -> Result<SuiEvent, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_event(id)).await
    }

    async fn get_events(
        &self,
        query: EventFilter,
        cursor: Option<EventID>,
        limit: Option<usize>,
        descending_order: bool,
    ) -> Result<EventPage, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_events(query, cursor, limit, descending_order))
        //     .await
    }

    async fn get_object_read(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<ObjectRead, IndexerError> {
        self.spawn_blocking(move |this| this.get_object_read(object_id, version))
            .await
    }

    async fn get_object(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<Option<Object>, IndexerError> {
        self.spawn_blocking(move |this| this.get_object(object_id, version))
            .await
    }

    async fn get_total_transaction_number_from_checkpoints(&self) -> Result<i64, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_total_transaction_number_from_checkpoints())
        //     .await
    }

    async fn get_transaction_by_digest(
        &self,
        tx_digest: &str,
    ) -> Result<SuiTransactionBlockResponse, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.get_transaction_by_digest(&tx_digest))
        //     .await
    }

    async fn multi_get_transactions_by_digests(
        &self,
        tx_digests: &[String],
    ) -> Result<Vec<SuiTransactionBlockResponse>, IndexerError> {
        unimplemented!()
        // self.spawn_blocking(move |this| this.multi_get_transactions_by_digests(&tx_digests))
        //     .await
    }

    async fn persist_checkpoints(
        &self,
        checkpoints: Vec<IndexedCheckpoint>,
    ) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_checkpoints(checkpoints))
            .await
    }

    async fn persist_transactions(
        &self,
        transactions: Vec<IndexedTransaction>,
    ) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_transactions(transactions))
            .await
    }

    async fn persist_object_changes(
        &self,
        tx_object_changes: Vec<TransactionObjectChangesV2>,
        // object_mutation_latency: Histogram,
        // object_deletion_latency: Histogram,
        // object_commit_chunk_counter: IntCounter,
    ) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| {
            this.persist_object_changes(
                tx_object_changes,
                // object_mutation_latency,
                // object_deletion_latency,
                // object_commit_chunk_counter,
            )
        })
        .await
    }

    async fn persist_events(&self, events: Vec<IndexedEvent>) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_events(events))
            .await
    }

    async fn persist_packages(&self, packages: Vec<IndexedPackage>) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_packages(packages))
            .await
    }

    async fn persist_tx_indices(&self, indices: Vec<TxIndex>) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_tx_indices(indices))
            .await
    }

    async fn persist_epoch(&self, data: TemporaryEpochStoreV2) -> Result<(), IndexerError> {
        self.spawn_blocking(move |this| this.persist_epoch(&data))
            .await
    }

    async fn get_network_total_transactions_previous_epoch(
        &self,
        epoch: u64,
    ) -> Result<u64, IndexerError> {
        self.spawn_blocking(move |this| this.get_network_total_transactions_previous_epoch(epoch))
            .await
    }

    async fn get_epochs(
        &self,
        cursor: Option<EpochId>,
        limit: usize,
        descending_order: Option<bool>,
    ) -> Result<Vec<EpochInfo>, IndexerError> {
        self.spawn_blocking(move |this| this.get_epochs(cursor, limit, descending_order))
            .await
    }

    async fn get_current_epoch(&self) -> Result<EpochInfo, IndexerError> {
        self.spawn_blocking(move |this| this.get_current_epoch())
            .await
    }

    fn module_cache(&self) -> Arc<Self::ModuleCache> {
        self.module_cache.clone()
    }

    fn indexer_metrics(&self) -> &IndexerMetrics {
        &self.metrics
    }
}

// fn persist_object_mutations(
//     conn: &mut PgConnection,
//     mutated_objects: Vec<Object>,
//     object_mutation_latency: Histogram,
//     object_commit_chunk_counter: IntCounter,
// ) -> Result<(), IndexerError> {
//     let mutated_objects = filter_latest_objects(mutated_objects);
//     let object_mutation_guard = object_mutation_latency.start_timer();
//     for mutated_object_change_chunk in mutated_objects.chunks(PG_COMMIT_CHUNK_SIZE) {
//         // bulk insert/update via UNNEST trick to bypass the 65535 parameters limit
//         // ref: https://klotzandrew.com/blog/postgres-passing-65535-parameter-limit
//         let insert_update_query =
//             compose_object_bulk_insert_update_query(mutated_object_change_chunk);
//         diesel::sql_query(insert_update_query)
//             .execute(conn)
//             .map_err(|e| {
//                 IndexerError::PostgresWriteError(format!(
//                     "Failed writing mutated objects to PostgresDB with error: {:?}. Chunk length: {}, total length: {}",
//                     e,
//                     mutated_object_change_chunk.len(),
//                     mutated_objects.len(),
//                 ))
//             })?;
//     }
//     object_mutation_guard.stop_and_record();
//     object_commit_chunk_counter.inc();
//     Ok(())
// }

// fn persist_object_deletions(
//     conn: &mut PgConnection,
//     deleted_objects: Vec<Object>,
//     object_deletion_latency: Histogram,
//     object_commit_chunk_counter: IntCounter,
// ) -> Result<(), IndexerError> {
//     let object_deletion_guard = object_deletion_latency.start_timer();
//     for deleted_object_change_chunk in deleted_objects.chunks(PG_COMMIT_CHUNK_SIZE) {
//         diesel::insert_into(objects::table)
//             .values(deleted_object_change_chunk)
//             .on_conflict(objects::object_id)
//             .do_update()
//             .set((
//                 objects::epoch.eq(excluded(objects::epoch)),
//                 objects::checkpoint.eq(excluded(objects::checkpoint)),
//                 objects::version.eq(excluded(objects::version)),
//                 objects::previous_transaction.eq(excluded(objects::previous_transaction)),
//                 objects::object_status.eq(excluded(objects::object_status)),
//             ))
//             .execute(conn)
//             .map_err(|e| {
//                 IndexerError::PostgresWriteError(format!(
//                     "Failed writing deleted objects to PostgresDB with error: {:?}. Chunk length: {}, total length: {}",
//                     e,
//                     deleted_object_change_chunk.len(),
//                     deleted_objects.len(),
//                 ))
//             })?;
//         object_commit_chunk_counter.inc();
//     }
//     object_deletion_guard.stop_and_record();
//     Ok(())
// }

fn get_objects_to_commit(
    tx_object_changes: Vec<TransactionObjectChangesV2>,
) -> (Vec<IndexedObject>, HashSet<ObjectID>) {
    let deleted_changes = tx_object_changes
        .iter()
        .flat_map(|changes| &changes.deleted_objects)
        .map(|o| o.0.clone())
        .collect::<HashSet<_>>();
    let mutated_objects = tx_object_changes
        .into_iter()
        .flat_map(|changes| changes.changed_objects);
    let mut latest_objects = HashMap::new();
    for object in mutated_objects {
        match latest_objects.entry(object.object_id) {
            Entry::Vacant(e) => {
                e.insert(object);
            }
            Entry::Occupied(mut e) => {
                if object.object_version > e.get().object_version {
                    e.insert(object);
                }
            }
        }
    }
    (latest_objects.into_values().collect(), deleted_changes)
}
