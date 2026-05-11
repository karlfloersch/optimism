//! Runtime cross-unsafe head RPC support.

use alloy_consensus::{BlockHeader, TxReceipt};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{B256, U64, keccak256};
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::{Filter, Log as RpcLog};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee_core::{RpcResult, async_trait};
use kona_interop::{RawMessagePayload, parse_log_to_executing_message};
use reth_provider::BlockReaderIdExt;
use reth_rpc_server_types::result::internal_rpc_err;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// A block head returned by `eth_crossUnsafeHead`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossUnsafeHead {
    /// Block number.
    pub number: U64,
    /// Block hash.
    pub hash: B256,
}

#[cfg_attr(not(test), rpc(server, namespace = "eth"))]
#[cfg_attr(test, rpc(server, client, namespace = "eth"))]
pub trait CrossUnsafeHeadApi {
    /// Returns the latest runtime-validated cross-unsafe head.
    #[method(name = "crossUnsafeHead")]
    async fn cross_unsafe_head(&self) -> RpcResult<CrossUnsafeHead>;

    /// Alias for callers that prefer a getter-style method name.
    #[method(name = "getCrossUnsafeHead")]
    async fn get_cross_unsafe_head(&self) -> RpcResult<CrossUnsafeHead>;
}

/// RPC extension that computes a simplified cross-unsafe head on demand.
#[derive(Debug, Clone)]
pub struct CrossUnsafeHeadExt<Provider> {
    inner: Arc<CrossUnsafeHeadExtInner<Provider>>,
}

#[derive(Debug)]
struct CrossUnsafeHeadExtInner<Provider> {
    provider: Provider,
    source_clients: SourceLogClients,
    state: Mutex<CrossUnsafeState>,
}

impl<Provider> CrossUnsafeHeadExt<Provider> {
    /// Creates a new runtime cross-unsafe head extension.
    pub fn new(
        provider: Provider,
        source_rpcs: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(CrossUnsafeHeadExtInner {
                provider,
                source_clients: SourceLogClients::new(source_rpcs)?,
                state: Mutex::new(CrossUnsafeState::default()),
            }),
        })
    }
}

#[async_trait]
impl<Provider> CrossUnsafeHeadApiServer for CrossUnsafeHeadExt<Provider>
where
    Provider: BlockReaderIdExt + Clone + Send + Sync + 'static,
{
    async fn cross_unsafe_head(&self) -> RpcResult<CrossUnsafeHead> {
        self.compute_cross_unsafe_head().await
    }

    async fn get_cross_unsafe_head(&self) -> RpcResult<CrossUnsafeHead> {
        self.compute_cross_unsafe_head().await
    }
}

impl<Provider> CrossUnsafeHeadExt<Provider>
where
    Provider: BlockReaderIdExt + Clone + Send + Sync + 'static,
{
    async fn compute_cross_unsafe_head(&self) -> RpcResult<CrossUnsafeHead> {
        let safe = self.safe_anchor()?;
        let latest = self
            .inner
            .provider
            .latest_header()
            .map_err(|err| internal_rpc_err(format!("failed to read latest header: {err}")))?
            .ok_or_else(|| internal_rpc_err("latest header not found"))?;

        let latest_number = latest.number();
        let latest_hash = latest.hash();

        let mut state = self.inner.state.lock().await;
        state.prune_before(safe.number);
        state.seed_safe(safe);
        self.rewind_cached_head_to_canonical(&mut state, safe.number)?;

        if latest_number <= state.head.number {
            return Ok(state.head.into());
        }

        let mut expected_parent = state.head.hash;
        for number in state.head.number.saturating_add(1)..=latest_number {
            let header = self
                .inner
                .provider
                .sealed_header(number)
                .map_err(|err| internal_rpc_err(format!("failed to read header {number}: {err}")))?
                .ok_or_else(|| internal_rpc_err(format!("header {number} not found")))?;

            let hash = header.hash();
            let parent_hash = header.parent_hash();
            if parent_hash != expected_parent {
                debug!(
                    target: "rpc::cross_unsafe",
                    number,
                    %hash,
                    %parent_hash,
                    expected = %expected_parent,
                    "stopping cross-unsafe walk at non-contiguous block"
                );
                state.remove_from(number);
                break;
            }

            if state.is_validated(number, hash, parent_hash) {
                expected_parent = hash;
                continue;
            }

            if !self.validate_block(number, header.timestamp()).await? {
                debug!(
                    target: "rpc::cross_unsafe",
                    number,
                    %hash,
                    "stopping cross-unsafe walk at unvalidated block"
                );
                state.remove_from(number);
                break;
            }

            state.insert(CachedBlock { number, hash, parent_hash });
            expected_parent = hash;
        }

        if latest_number == state.head.number && latest_hash != state.head.hash {
            state.remove_from(latest_number);
        }

        Ok(state.head.into())
    }

    fn rewind_cached_head_to_canonical(
        &self,
        state: &mut CrossUnsafeState,
        safe_number: u64,
    ) -> RpcResult<()> {
        while state.head.number > safe_number {
            let number = state.head.number;
            let Some(header) = self.inner.provider.sealed_header(number).map_err(|err| {
                internal_rpc_err(format!("failed to read cached head header {number}: {err}"))
            })?
            else {
                state.remove_from(number);
                continue;
            };

            if header.hash() == state.head.hash {
                break;
            }

            state.remove_from(number);
        }

        Ok(())
    }

    fn safe_anchor(&self) -> RpcResult<CachedBlock> {
        if let Some(safe) = self
            .inner
            .provider
            .safe_block_num_hash()
            .map_err(|err| internal_rpc_err(format!("failed to read safe block: {err}")))?
        {
            return Ok(CachedBlock {
                number: safe.number,
                hash: safe.hash,
                parent_hash: B256::ZERO,
            });
        }

        let genesis = self
            .inner
            .provider
            .sealed_header(0)
            .map_err(|err| internal_rpc_err(format!("failed to read genesis header: {err}")))?
            .ok_or_else(|| internal_rpc_err("safe block unavailable and genesis not found"))?;

        Ok(CachedBlock { number: 0, hash: genesis.hash(), parent_hash: genesis.parent_hash() })
    }

    async fn validate_block(&self, number: u64, executing_timestamp: u64) -> RpcResult<bool> {
        let receipts = self
            .inner
            .provider
            .receipts_by_block(BlockHashOrNumber::Number(number))
            .map_err(|err| {
                internal_rpc_err(format!("failed to read receipts for block {number}: {err}"))
            })?
            .ok_or_else(|| internal_rpc_err(format!("receipts for block {number} not found")))?;

        for receipt in receipts {
            for log in receipt.logs() {
                let Some(message) = parse_log_to_executing_message(log) else { continue };

                let initiating_timestamp = message.identifier.timestamp.saturating_to::<u64>();
                if initiating_timestamp > executing_timestamp {
                    debug!(
                        target: "rpc::cross_unsafe",
                        number,
                        initiating_timestamp,
                        executing_timestamp,
                        "executing message points to a future initiating timestamp"
                    );
                    return Ok(false);
                }

                let source_chain_id = message.identifier.chainId.saturating_to::<u64>();
                let source_block = message.identifier.blockNumber.saturating_to::<u64>();
                let source_log_index = message.identifier.logIndex.saturating_to::<u64>();
                let exists = self
                    .inner
                    .source_clients
                    .initiating_message_exists(
                        source_chain_id,
                        source_block,
                        source_log_index,
                        message.identifier.origin,
                        message.payloadHash,
                    )
                    .await;

                if !exists {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct SourceLogClients {
    clients: BTreeMap<u64, SourceLogClient>,
}

impl SourceLogClients {
    fn new(source_rpcs: impl IntoIterator<Item = impl Into<String>>) -> Result<Self, String> {
        let mut clients = BTreeMap::new();
        for source_rpc in source_rpcs {
            let source_rpc = source_rpc.into();
            let (chain_id, endpoint) = source_rpc.split_once('=').ok_or_else(|| {
                format!("invalid source RPC mapping {source_rpc:?}: expected CHAIN_ID=RPC_URL")
            })?;
            let chain_id = chain_id
                .parse::<u64>()
                .map_err(|err| format!("invalid source chain ID {chain_id:?}: {err}"))?;
            if clients.insert(chain_id, SourceLogClient::new(endpoint.to_string())?).is_some() {
                return Err(format!("duplicate source RPC for chain ID {chain_id}"));
            }
        }

        Ok(Self { clients })
    }

    async fn initiating_message_exists(
        &self,
        chain_id: u64,
        block_number: u64,
        log_index: u64,
        origin: alloy_primitives::Address,
        payload_hash: B256,
    ) -> bool {
        let Some(client) = self.clients.get(&chain_id) else {
            warn!(
                target: "rpc::cross_unsafe",
                chain_id,
                block_number,
                log_index,
                "missing source RPC for initiating message chain"
            );
            return false;
        };

        client
            .initiating_message_exists(chain_id, block_number, log_index, origin, payload_hash)
            .await
    }
}

#[derive(Debug, Clone)]
struct SourceLogClient {
    endpoint: String,
    client: RpcClient,
}

impl SourceLogClient {
    fn new(endpoint: String) -> Result<Self, String> {
        let url = endpoint
            .parse()
            .map_err(|err| format!("invalid cross unsafe head source RPC URL: {err}"))?;
        Ok(Self { client: RpcClient::new_http(url), endpoint })
    }

    async fn initiating_message_exists(
        &self,
        chain_id: u64,
        block_number: u64,
        log_index: u64,
        origin: alloy_primitives::Address,
        payload_hash: B256,
    ) -> bool {
        let filter = Filter::new().select(block_number);
        let logs = match self.client.request::<_, Vec<RpcLog>>("eth_getLogs", (filter,)).await {
            Ok(logs) => logs,
            Err(err) => {
                warn!(
                    target: "rpc::cross_unsafe",
                    %err,
                    endpoint = %self.endpoint,
                    chain_id,
                    block_number,
                    "failed to fetch source logs"
                );
                return false;
            }
        };

        let Some(log) = logs.into_iter().find(|log| log.log_index == Some(log_index)) else {
            debug!(
                target: "rpc::cross_unsafe",
                chain_id,
                block_number,
                log_index,
                "source log not found"
            );
            return false;
        };

        if log.address() != origin {
            debug!(
                target: "rpc::cross_unsafe",
                chain_id,
                block_number,
                log_index,
                expected = %origin,
                actual = %log.address(),
                "source log origin mismatch"
            );
            return false;
        }

        let remote_payload = RawMessagePayload::from(&log.inner);
        let remote_payload_hash = keccak256(remote_payload.as_ref());
        if remote_payload_hash != payload_hash {
            debug!(
                target: "rpc::cross_unsafe",
                chain_id,
                block_number,
                log_index,
                expected = %payload_hash,
                actual = %remote_payload_hash,
                "source log payload hash mismatch"
            );
            return false;
        }

        true
    }
}

#[derive(Debug, Default)]
struct CrossUnsafeState {
    head: CachedBlock,
    validated: BTreeMap<u64, CachedBlock>,
}

impl CrossUnsafeState {
    fn seed_safe(&mut self, safe: CachedBlock) {
        if self.head.number <= safe.number {
            self.head = safe;
        }
        self.validated.insert(safe.number, safe);
    }

    fn prune_before(&mut self, safe_number: u64) {
        self.validated.retain(|number, _| *number >= safe_number);
    }

    fn is_validated(&self, number: u64, hash: B256, parent_hash: B256) -> bool {
        self.validated
            .get(&number)
            .is_some_and(|block| block.hash == hash && block.parent_hash == parent_hash)
    }

    fn insert(&mut self, block: CachedBlock) {
        self.head = block;
        self.validated.insert(block.number, block);
    }

    fn remove_from(&mut self, number: u64) {
        self.validated.split_off(&number);
        if self.head.number >= number {
            self.head = self.validated.values().next_back().copied().unwrap_or_default();
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CachedBlock {
    number: u64,
    hash: B256,
    parent_hash: B256,
}

impl From<CachedBlock> for CrossUnsafeHead {
    fn from(value: CachedBlock) -> Self {
        Self { number: U64::from(value.number), hash: value.hash }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_source_rpc_mappings_by_chain_id() {
        let clients = SourceLogClients::new([
            "901=http://chain-a:8545".to_string(),
            "902=http://chain-b:8545".to_string(),
        ])
        .unwrap();

        assert!(clients.clients.contains_key(&901));
        assert!(clients.clients.contains_key(&902));
    }

    #[test]
    fn rejects_invalid_source_rpc_mappings() {
        assert!(SourceLogClients::new(["http://chain-a:8545".to_string()]).is_err());
        assert!(SourceLogClients::new(["chain-a=http://chain-a:8545".to_string()]).is_err());
        assert!(SourceLogClients::new(["901=not a url".to_string()]).is_err());
        assert!(
            SourceLogClients::new([
                "901=http://chain-a:8545".to_string(),
                "901=http://chain-b:8545".to_string(),
            ])
            .is_err()
        );
    }
}
