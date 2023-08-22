// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use fastcrypto::hash::Hash;
use move_binary_format::CompiledModule;
use move_bytecode_utils::module_cache::GetModule;
use move_core_types::language_storage::ModuleId;
use prometheus::{Histogram, IntCounter};
use serde_json::value::Index;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sui_json_rpc_types::{
    Checkpoint as RpcCheckpoint, CheckpointId, EpochInfo, EventFilter, EventPage, SuiEvent,
    SuiTransactionBlockResponse,
};
use sui_types::base_types::{EpochId, ObjectID, ObjectRef, SequenceNumber};
use sui_types::digests::CheckpointDigest;
use sui_types::event::EventID;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::object::{Object, ObjectRead};

use crate::errors::IndexerError;
use crate::handlers::checkpoint_handler_v2::InMemObjectCache;
use crate::metrics::IndexerMetrics;

use crate::types_v2::{
    IndexedCheckpoint, IndexedEndOfEpochInfo, IndexedEpochInfo, IndexedEvent, IndexedObject,
    IndexedPackage, IndexedTransaction, TxIndex,
};

#[async_trait]
pub trait IndexerStoreV2 {
    type ModuleCache: GetModule<Item = Arc<CompiledModule>, Error = anyhow::Error>
        + Send
        + Sync
        + 'static;

    async fn get_latest_tx_checkpoint_sequence_number(&self) -> Result<Option<u64>, IndexerError>;
    // async fn get_latest_object_checkpoint_sequence_number(&self) -> Result<i64, IndexerError>;
    async fn get_checkpoint(&self, id: CheckpointId) -> Result<RpcCheckpoint, IndexerError>;
    async fn get_checkpoints(
        &self,
        cursor: Option<CheckpointId>,
        limit: usize,
    ) -> Result<Vec<RpcCheckpoint>, IndexerError>;

    async fn get_checkpoint_sequence_number(
        &self,
        digest: CheckpointDigest,
    ) -> Result<CheckpointSequenceNumber, IndexerError>;

    async fn get_event(&self, id: EventID) -> Result<SuiEvent, IndexerError>;
    async fn get_events(
        &self,
        query: EventFilter,
        cursor: Option<EventID>,
        limit: Option<usize>,
        descending_order: bool,
    ) -> Result<EventPage, IndexerError>;

    async fn get_object_read(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<ObjectRead, IndexerError>;

    async fn get_object(
        &self,
        object_id: ObjectID,
        version: Option<SequenceNumber>,
    ) -> Result<Option<Object>, IndexerError>;

    async fn get_total_transaction_number_from_checkpoints(&self) -> Result<i64, IndexerError>;

    // TODO: combine all get_transaction* methods
    async fn get_transaction_by_digest(
        &self,
        tx_digest: &str,
    ) -> Result<SuiTransactionBlockResponse, IndexerError>;

    async fn multi_get_transactions_by_digests(
        &self,
        tx_digests: &[String],
    ) -> Result<Vec<SuiTransactionBlockResponse>, IndexerError>;

    // async fn compose_sui_transaction_block_response(
    //     &self,
    //     tx: Transaction,
    //     options: Option<&SuiTransactionBlockResponseOptions>,
    // ) -> Result<SuiTransactionBlockResponse, IndexerError>;

    // async fn get_all_transaction_page(
    //     &self,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_checkpoint(
    //     &self,
    //     checkpoint_sequence_number: i64,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_transaction_kinds(
    //     &self,
    //     kind_names: Vec<String>,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_sender_address(
    //     &self,
    //     sender_address: String,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_recipient_address(
    //     &self,
    //     sender_address: Option<SuiAddress>,
    //     recipient_address: SuiAddress,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // `address` can be either sender or recipient address of the transaction
    // async fn get_transaction_page_by_address(
    //     &self,
    //     address: SuiAddress,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_input_object(
    //     &self,
    //     object_id: ObjectID,
    //     version: Option<i64>,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_changed_object(
    //     &self,
    //     object_id: ObjectID,
    //     version: Option<i64>,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    // async fn get_transaction_page_by_move_call(
    //     &self,
    //     package: ObjectID,
    //     module: Option<Identifier>,
    //     function: Option<Identifier>,
    //     start_sequence: Option<i64>,
    //     limit: usize,
    //     is_descending: bool,
    // ) -> Result<Vec<Transaction>, IndexerError>;

    async fn persist_checkpoints(
        &self,
        // checkpoints: &[IndexedCheckpoint],
        checkpoints: Vec<IndexedCheckpoint>,
        // counter_committed_tx: IntCounter,
    ) -> Result<(), IndexerError>;

    async fn persist_transactions(
        &self,
        transactions: Vec<IndexedTransaction>,
        // counter_committed_tx: IntCounter,
    ) -> Result<(), IndexerError>;

    async fn persist_tx_indices(
        &self,
        indices: Vec<TxIndex>,
        // counter_committed_tx: IntCounter,
    ) -> Result<(), IndexerError>;

    async fn persist_object_changes(
        &self,
        tx_object_changes: Vec<TransactionObjectChangesV2>,
        // object_mutation_latency: Histogram,
        // object_deletion_latency: Histogram,
        // object_commit_chunk_counter: IntCounter,
    ) -> Result<(), IndexerError>;

    async fn persist_events(&self, events: Vec<IndexedEvent>) -> Result<(), IndexerError>;

    async fn persist_packages(&self, packages: Vec<IndexedPackage>) -> Result<(), IndexerError>;

    // NOTE: these tables are for tx query performance optimization
    // async fn persist_transaction_index_tables(
    //     &self,
    //     input_objects: &[InputObject],
    //     changed_objects: &[ChangedObject],
    //     move_calls: &[MoveCall],
    //     recipients: &[Recipient],
    // ) -> Result<(), IndexerError>;

    async fn persist_epoch(&self, data: TemporaryEpochStoreV2) -> Result<(), IndexerError>;

    async fn get_checkpoint_ending_tx_sequence_number(
        &self,
        seq_num: CheckpointSequenceNumber,
    ) -> Result<Option<u64>, IndexerError>;

    async fn get_network_total_transactions_previous_epoch(
        &self,
        epoch: u64,
    ) -> Result<u64, IndexerError>;

    async fn get_epochs(
        &self,
        cursor: Option<EpochId>,
        limit: usize,
        descending_order: Option<bool>,
    ) -> Result<Vec<EpochInfo>, IndexerError>;

    async fn get_current_epoch(&self) -> Result<EpochInfo, IndexerError>;

    fn module_cache(&self) -> Arc<Self::ModuleCache>;

    fn indexer_metrics(&self) -> &IndexerMetrics;
}

// Per checkpoint indexing
#[derive(Debug)]
pub struct TemporaryCheckpointStoreV2 {
    pub checkpoint: IndexedCheckpoint,
    pub transactions: Vec<IndexedTransaction>,
    pub events: Vec<IndexedEvent>,
    pub tx_indices: Vec<TxIndex>,
    pub object_changes: TransactionObjectChangesV2,
    pub packages: Vec<IndexedPackage>,
}

#[derive(Debug)]
pub struct TransactionObjectChangesV2 {
    pub changed_objects: Vec<IndexedObject>,
    pub deleted_objects: Vec<ObjectRef>,
}

// Per epoch indexing
#[derive(Debug)]
pub struct TemporaryEpochStoreV2 {
    pub last_epoch: Option<IndexedEndOfEpochInfo>,
    pub new_epoch: IndexedEpochInfo,
}

pub struct InterimModuleResolver<GM>
where
    GM: GetModule<Item = Arc<CompiledModule>, Error = anyhow::Error>,
{
    backup: GM,
    object_cache: Arc<Mutex<InMemObjectCache>>,
    // packages: HashMap<String, Arc<CompiledModule>>,
}

impl<GM> InterimModuleResolver<GM>
where
    GM: GetModule<Item = Arc<CompiledModule>, Error = anyhow::Error>,
{
    pub fn new(backup: GM, object_cache: Arc<Mutex<InMemObjectCache>>, new_packages: &Vec<IndexedPackage>) -> Self {
        object_cache.lock().unwrap().insert_packages(new_packages);
        Self {
            backup,
            object_cache,
            // packages: HashMap::new(),
        }
    }
}

impl<GM> GetModule for InterimModuleResolver<GM>
where
    GM: GetModule<Item = Arc<CompiledModule>, Error = anyhow::Error>,
{
    type Error = IndexerError;
    type Item = Arc<CompiledModule>;

    fn get_module_by_id(&self, id: &ModuleId) -> Result<Option<Arc<CompiledModule>>, Self::Error> {
        // let name = id.name().to_string();
        // tracing::error!("InterimModuleResolver get_module_by_id: {name}");
        if let Some(m) = self.object_cache.lock().unwrap().get_module_by_id(id) {
            Ok(Some(m.clone()))
        } else {
            self.backup
                .get_module_by_id(id)
                .map_err(|e| IndexerError::ModuleResolutionError(e.to_string()))
        }
    }
}
