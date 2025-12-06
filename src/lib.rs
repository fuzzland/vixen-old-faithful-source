//! Old Faithful CAR source for Vixen.
//!
//! This source streams transactions stored in Old Faithful CAR archives and emits
//! `SubscribeUpdate` messages compatible with Vixen's runtime.

pub mod car;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::Stdio,
};

use async_trait::async_trait;
use bincode::Options;
use clap::Args;
use prost::Message;
use solana_message::{compiled_instruction::CompiledInstruction, v0::MessageAddressTableLookup, VersionedMessage};
use solana_sdk::{pubkey::Pubkey as SolPubkey, transaction::{TransactionError, VersionedTransaction}};
use tokio::{fs, io::BufReader, process::Command, sync::mpsc::Sender};
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateSlot,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
    },
    solana::storage::confirmed_block::{
        CompiledInstruction as ProtoCompiledInstruction, Message as ProtoMessage,
        MessageAddressTableLookup as ProtoMessageAddressTableLookup,
        MessageHeader as ProtoHeader, Rewards, Transaction as ProtoTransaction,
        TransactionStatusMeta,
    },
    tonic::Status,
};
use yellowstone_vixen::{sources::SourceTrait, Error as VixenError};
use yellowstone_vixen_core::{Filters, Pubkey, TransactionPrefilter};

use crate::car::node::{
    Block, Node, NodeError, NodeReader, Nodes, ReassableError, Transaction as CarTransaction,
};

/// Configuration for the Old Faithful CAR source.
#[derive(Debug, Clone, Args, serde::Deserialize)]
pub struct OldFaithfulCarConfig {
    /// Directory where CAR files will be stored or read from.
    #[arg(long, env, default_value = "./old-faithful-car")]
    pub data_dir: PathBuf,
    /// Epochs to download and stream, comma separated.
    #[arg(long, env, value_delimiter = ',', required = true)]
    pub epochs: Vec<u64>,
    /// Stop after emitting this many blocks (useful for tests).
    #[arg(long, env)]
    pub max_blocks: Option<usize>,
    /// Stop after emitting this many transactions.
    #[arg(long, env)]
    pub max_transactions: Option<usize>,
    /// Skip downloading missing CAR files (error if file not found).
    #[arg(long, env, default_value_t = false)]
    pub skip_download: bool,
}

/// Source implementation that reads Old Faithful CAR files.
#[derive(Debug)]
pub struct OldFaithfulCarSource {
    filters: Filters,
    config: OldFaithfulCarConfig,
}

const VOTE_PROGRAM_ID: SolPubkey = solana_sdk::pubkey!("Vote111111111111111111111111111111111111111");

#[async_trait]
impl SourceTrait for OldFaithfulCarSource {
    type Config = OldFaithfulCarConfig;

    fn new(config: Self::Config, filters: Filters) -> Self { Self { filters, config } }

    async fn connect(&self, tx: Sender<Result<SubscribeUpdate, Status>>) -> Result<(), VixenError> {
        stream_epochs(&self.config, &self.filters, tx)
            .await
            .map_err(|e| VixenError::Other(Box::new(e)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CarSourceError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Reassemble(#[from] ReassableError),
    #[error(transparent)]
    ProstDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error("channel closed while sending updates")]
    ChannelClosed,
    #[error("missing node: {0}")]
    MissingNode(String),
    #[error("failed to download epoch file: {0}")]
    DownloadFailed(String),
    #[error("metadata decode failed: {0}")]
    MetaDecode(String),
}

#[derive(serde::Deserialize)]
struct StoredTransactionStatusMeta {
    err: Result<(), TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
struct StoredBlockReward {
    pubkey: String,
    lamports: i64,
}

async fn stream_epochs(
    config: &OldFaithfulCarConfig,
    filters: &Filters,
    tx: Sender<Result<SubscribeUpdate, Status>>,
) -> Result<(), CarSourceError> {
    if config.epochs.is_empty() {
        return Err(CarSourceError::DownloadFailed(
            "no epochs provided".to_string(),
        ));
    }

    for epoch in &config.epochs {
        let path = download_epoch_if_needed(*epoch, &config.data_dir, config.skip_download).await?;
        info!(epoch, path = %path.display(), "processing epoch");
        process_car_file(&path, filters, &tx, config).await?;
    }

    Ok(())
}

async fn download_epoch_if_needed(
    epoch: u64,
    data_dir: &Path,
    skip_download: bool,
) -> Result<PathBuf, CarSourceError> {
    let file_name = format!("epoch-{epoch}.car");
    let output_path = data_dir.join(&file_name);
    if output_path.exists() {
        debug!(path = %output_path.display(), "using cached CAR file");
        return Ok(output_path);
    }
    if skip_download {
        return Err(CarSourceError::DownloadFailed(format!(
            "file missing: {} (skip_download=true)",
            output_path.display()
        )));
    }

    fs::create_dir_all(data_dir).await?;
    let tmp_dir = data_dir.join("tmp");
    fs::create_dir_all(&tmp_dir).await?;

    let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
    let tmp_target = tmp_dir.join(&file_name);

    info!(%url, "downloading CAR");
    let status = Command::new("aria2c")
        .args([
            "-s32",
            "-x16",
            "-k300M",
            &url,
            "-d",
            tmp_dir
                .to_str()
                .ok_or_else(|| CarSourceError::DownloadFailed("invalid tmp dir".into()))?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .map_err(|err| CarSourceError::DownloadFailed(format!("aria2c failed: {err}")))?;

    if !status.success() {
        return Err(CarSourceError::DownloadFailed(format!(
            "aria2c exited with {status}"
        )));
    }

    fs::rename(tmp_target, &output_path).await?;
    Ok(output_path)
}

async fn process_car_file(
    path: &Path,
    filters: &Filters,
    tx: &Sender<Result<SubscribeUpdate, Status>>,
    config: &OldFaithfulCarConfig,
) -> Result<(), CarSourceError> {
    let file = fs::File::open(path).await?;
    let mut reader = NodeReader::new(BufReader::new(file));

    let mut blocks_seen = 0usize;
    let mut txs_sent = 0usize;

    loop {
        let nodes = Nodes::read_until_block(&mut reader).await?;
        if nodes.nodes.is_empty() {
            break;
        }
        blocks_seen += 1;

        process_block(&nodes, filters, tx, &mut txs_sent, config.max_transactions).await?;

        if let Some(max) = config.max_blocks {
            if blocks_seen >= max {
                break;
            }
        }
        if let Some(max) = config.max_transactions {
            if txs_sent >= max {
                break;
            }
        }
    }

    Ok(())
}

async fn process_block(
    nodes: &Nodes,
    filters: &Filters,
    tx: &Sender<Result<SubscribeUpdate, Status>>,
    txs_sent: &mut usize,
    max_txs: Option<usize>,
) -> Result<(), CarSourceError> {
    let block = find_block(nodes).ok_or_else(|| CarSourceError::MissingNode("Block".into()))?;

    send_slot_update_if_needed(block, filters, tx).await?;

    for entry_cid in &block.entries {
        let entry = match nodes.nodes.get(entry_cid) {
            Some(Node::Entry(entry)) => entry,
            Some(_) => return Err(CarSourceError::MissingNode("Entry".into())),
            None => return Err(CarSourceError::MissingNode("Entry".into())),
        };

        for txn_cid in &entry.transactions {
            if let Some(max) = max_txs {
                if *txs_sent >= max {
                    return Ok(());
                }
            }
            let txn = match nodes.nodes.get(txn_cid) {
                Some(Node::Transaction(txn)) => txn,
                Some(_) => return Err(CarSourceError::MissingNode("Transaction".into())),
                None => return Err(CarSourceError::MissingNode("Transaction".into())),
            };

            emit_transaction_update(txn, nodes, filters, tx).await?;
            *txs_sent += 1;
        }
    }

    Ok(())
}

async fn send_slot_update_if_needed(
    block: &Block,
    filters: &Filters,
    tx: &Sender<Result<SubscribeUpdate, Status>>,
) -> Result<(), CarSourceError> {
    let slot_filters: Vec<String> = filters
        .parsers_filters
        .iter()
        .filter_map(|(id, pre)| pre.slot.as_ref().map(|_| id.clone()))
        .collect();

    if slot_filters.is_empty() {
        return Ok(());
    }

    let slot_update = SubscribeUpdate {
        filters: slot_filters,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: block.slot,
            parent: Some(block.meta.parent_slot),
            status: SlotStatus::SlotFinalized.into(),
            dead_error: None,
        })),
        created_at: None,
    };

    tx.send(Ok(slot_update))
        .await
        .map_err(|_| CarSourceError::ChannelClosed)
}

async fn emit_transaction_update(
    car_tx: &CarTransaction,
    nodes: &Nodes,
    filters: &Filters,
    tx: &Sender<Result<SubscribeUpdate, Status>>,
) -> Result<(), CarSourceError> {
    let tx_bytes = nodes.reassemble_dataframes(&car_tx.data)?;
    let versioned_tx: VersionedTransaction = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .deserialize(&tx_bytes)?;

    let meta_bytes = nodes.reassemble_dataframes(&car_tx.metadata)?;
    let meta = decode_metadata(&meta_bytes)?;

    let account_keys = gather_accounts(&versioned_tx, meta.as_ref());
    let filter_ids = matching_transaction_filters(filters, &account_keys);
    if filter_ids.is_empty() {
        return Ok(());
    }

    let tx_proto = to_proto_transaction(&versioned_tx);

    let info = SubscribeUpdateTransactionInfo {
        signature: versioned_tx
            .signatures
            .get(0)
            .map(|s| s.as_ref().to_vec())
            .unwrap_or_default(),
        is_vote: is_vote_transaction(&versioned_tx),
        transaction: Some(tx_proto),
        meta,
        index: car_tx.index.unwrap_or_default(),
    };

    let update = SubscribeUpdate {
        filters: filter_ids,
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(info),
            slot: car_tx.slot,
        })),
        created_at: None,
    };

    tx.send(Ok(update))
        .await
        .map_err(|_| CarSourceError::ChannelClosed)
}

fn find_block(nodes: &Nodes) -> Option<&Block> {
    nodes
        .nodes
        .values()
        .find_map(|node| if let Node::Block(block) = node { Some(block) } else { None })
}

fn gather_accounts(
    versioned_tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
) -> Vec<Pubkey> {
    let mut accounts = Vec::new();

    for key in static_account_keys(versioned_tx) {
        accounts.push(key.to_bytes().into());
    }

    if let Some(meta) = meta {
        for key in meta
            .loaded_writable_addresses
            .iter()
            .chain(meta.loaded_readonly_addresses.iter())
        {
            if let Ok(bytes) = <[u8; 32]>::try_from(key.as_slice()) {
                accounts.push(bytes.into());
            }
        }
    }

    accounts
}

fn static_account_keys(tx: &VersionedTransaction) -> Vec<SolPubkey> {
    match &tx.message {
        VersionedMessage::Legacy(message) => message.account_keys.clone(),
        VersionedMessage::V0(message) => message.account_keys.clone(),
    }
}

fn is_vote_transaction(tx: &VersionedTransaction) -> bool {
    let program_id = match &tx.message {
        VersionedMessage::Legacy(message) => message
            .instructions
            .first()
            .and_then(|ix| message.account_keys.get(ix.program_id_index as usize)),
        VersionedMessage::V0(message) => message
            .instructions
            .first()
            .and_then(|ix| message.account_keys.get(ix.program_id_index as usize)),
    };

    program_id.is_some_and(|id| *id == VOTE_PROGRAM_ID)
}

fn to_proto_transaction(tx: &VersionedTransaction) -> ProtoTransaction {
    ProtoTransaction {
        signatures: tx
            .signatures
            .iter()
            .map(|signature| signature.as_ref().to_vec())
            .collect(),
        message: Some(to_proto_message(&tx.message)),
    }
}

fn to_proto_message(message: &VersionedMessage) -> ProtoMessage {
    match message {
        VersionedMessage::Legacy(message) => ProtoMessage {
            header: Some(to_proto_header(&message.header)),
            account_keys: message.account_keys.iter().map(|k| k.to_bytes().to_vec()).collect(),
            recent_blockhash: message.recent_blockhash.to_bytes().to_vec(),
            instructions: message.instructions.iter().map(to_proto_instruction).collect(),
            versioned: false,
            address_table_lookups: vec![],
        },
        VersionedMessage::V0(message) => ProtoMessage {
            header: Some(to_proto_header(&message.header)),
            account_keys: message.account_keys.iter().map(|k| k.to_bytes().to_vec()).collect(),
            recent_blockhash: message.recent_blockhash.to_bytes().to_vec(),
            instructions: message.instructions.iter().map(to_proto_instruction).collect(),
            versioned: true,
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(to_proto_lookup)
                .collect(),
        },
    }
}

fn to_proto_header(header: &solana_sdk::message::MessageHeader) -> ProtoHeader {
    ProtoHeader {
        num_required_signatures: header.num_required_signatures as u32,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
    }
}

fn to_proto_instruction(ix: &CompiledInstruction) -> ProtoCompiledInstruction {
    ProtoCompiledInstruction {
        program_id_index: ix.program_id_index as u32,
        accounts: ix.accounts.clone(),
        data: ix.data.clone(),
    }
}

fn to_proto_lookup(lookup: &MessageAddressTableLookup) -> ProtoMessageAddressTableLookup {
    ProtoMessageAddressTableLookup {
        account_key: lookup.account_key.to_bytes().to_vec(),
        writable_indexes: lookup.writable_indexes.clone(),
        readonly_indexes: lookup.readonly_indexes.clone(),
    }
}

fn matching_transaction_filters(filters: &Filters, accounts: &[Pubkey]) -> Vec<String> {
    let account_set: HashSet<Pubkey> = accounts.iter().copied().collect();

    filters
        .parsers_filters
        .iter()
        .filter_map(|(id, prefilter)| {
            let tx_filter = prefilter.transaction.as_ref()?;
            match_transaction_prefilter(tx_filter, accounts, &account_set).then_some(id.clone())
        })
        .collect()
}

fn match_transaction_prefilter(
    filter: &TransactionPrefilter,
    accounts: &[Pubkey],
    account_set: &HashSet<Pubkey>,
) -> bool {
    if !filter.accounts_include.is_empty()
        && !accounts
            .iter()
            .any(|account| filter.accounts_include.contains(account))
    {
        return false;
    }

    if !filter.accounts_required.is_empty()
        && !filter
            .accounts_required
            .iter()
            .all(|account| account_set.contains(account))
    {
        return false;
    }

    true
}

fn decode_metadata(
    meta_bytes: &[u8],
) -> Result<Option<TransactionStatusMeta>, CarSourceError> {
    if meta_bytes.is_empty() {
        return Ok(None);
    }

    let decompressed = match zstd::decode_all(meta_bytes) {
        Ok(buf) => buf,
        Err(err) => {
            // Maybe the payload is already uncompressed.
            warn!(%err, "failed to decompress metadata with zstd, trying raw bytes");
            meta_bytes.to_vec()
        },
    };

    if decompressed.is_empty() {
        return Ok(None);
    }

    if let Ok(meta) = TransactionStatusMeta::decode(decompressed.as_slice()) {
        return Ok(Some(meta));
    }

    match bincode::deserialize::<StoredTransactionStatusMeta>(&decompressed) {
        Ok(stored) => Ok(Some(meta_from_stored(stored))),
        Err(err) => Err(CarSourceError::MetaDecode(err.to_string())),
    }
}

fn meta_from_stored(stored: StoredTransactionStatusMeta) -> TransactionStatusMeta {
    let err = match stored.err {
        Ok(()) => None,
        Err(err) => Some(yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionError {
            err: bincode::serialize(&err).unwrap_or_default(),
        }),
    };

    TransactionStatusMeta {
        err,
        fee: stored.fee,
        pre_balances: stored.pre_balances,
        post_balances: stored.post_balances,
        inner_instructions: vec![],
        inner_instructions_none: true,
        log_messages: vec![],
        log_messages_none: true,
        pre_token_balances: vec![],
        post_token_balances: vec![],
        rewards: vec![],
        loaded_writable_addresses: vec![],
        loaded_readonly_addresses: vec![],
        return_data: None,
        return_data_none: true,
        compute_units_consumed: None,
        cost_units: None,
    }
}

fn _decode_rewards(data: &[u8]) -> Result<Rewards, CarSourceError> {
    // Helper kept for parity with the reference tool; not used directly today.
    let decompressed = zstd::decode_all(data)?;
    if let Ok(rewards) = Rewards::decode(decompressed.as_slice()) {
        return Ok(rewards);
    }

    let stored: Vec<StoredBlockReward> = bincode::deserialize(&decompressed)?;
    let rewards = Rewards {
        rewards: stored
            .into_iter()
            .map(|r| yellowstone_grpc_proto::solana::storage::confirmed_block::Reward {
                pubkey: r.pubkey,
                lamports: r.lamports,
                post_balance: 0,
                reward_type: 0,
                commission: String::new(),
            })
            .collect(),
        num_partitions: None,
    };
    Ok(rewards)
}
