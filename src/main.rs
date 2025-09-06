use std::sync::{Arc, atomic::AtomicU64};

use anyhow::Result;
use clap::Parser;
use dotenv::dotenv;
use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use log::{info, warn};
use near_crypto::{InMemorySigner, Signer};
use near_jsonrpc_client::{
    JsonRpcClient,
    methods::{
        broadcast_tx_async::RpcBroadcastTxAsyncRequest,
        query::{RpcQueryRequest, RpcQueryResponse},
    },
};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::{
    action::{Action, FunctionCallAction},
    hash::CryptoHash,
    transaction::{Transaction, TransactionV0},
    types::{AccountId, BlockReference},
};
use omni_types::OmniAddress;
use tokio::{
    sync::{RwLock, Semaphore},
    time::Duration,
};

const FT_TRANSFER_CALL_GAS: u64 = 300_000_000_000_000;
const FT_TRANSFER_CALL_DEPOSIT: u128 = 1;

const MAX_IN_FLIGHT: usize = 100;

#[derive(Parser)]
struct CliArgs {
    /// Amount to transfer
    #[clap(long)]
    amount: u128,

    /// Recipient address
    #[clap(long)]
    recipient: OmniAddress,

    /// Fee to pay
    #[clap(long)]
    fee: u128,

    /// Omni bridge account ID
    #[clap(long)]
    omni_bridge: AccountId,

    /// Token account ID
    #[clap(long)]
    token: AccountId,

    /// Duration in seconds
    #[clap(long)]
    duration: u64,

    /// Reset block hash interval in seconds
    #[clap(long, default_value = "30")]
    reset_block_hash_interval: u64,
}

struct Client {
    jsonrpc_client: JsonRpcClient,
    signer: InMemorySigner,
    nonce: AtomicU64,
    block_hash: RwLock<CryptoHash>,
}

impl Client {
    fn new(rpc_url: String) -> Result<Self> {
        let Signer::InMemory(signer) = InMemorySigner::from_secret_key(
            std::env::var("ACCOUNT_ID")?.parse()?,
            std::env::var("PRIVATE_KEY")?.parse()?,
        ) else {
            anyhow::bail!("Unsupported signer type");
        };
        Ok(Self {
            jsonrpc_client: JsonRpcClient::connect(rpc_url),
            signer,
            nonce: AtomicU64::new(0),
            block_hash: RwLock::new(CryptoHash::default()),
        })
    }

    async fn get_access_key_query(&self) -> Result<RpcQueryResponse> {
        let rpc_request = RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: near_primitives::views::QueryRequest::ViewAccessKey {
                account_id: self.signer.account_id.clone(),
                public_key: self.signer.public_key.clone(),
            },
        };

        self.jsonrpc_client
            .call(&rpc_request)
            .await
            .map_err(Into::into)
    }

    async fn reset_block_hash(&self) -> Result<()> {
        let access_key_query_response = self.get_access_key_query().await?;
        *self.block_hash.write().await = access_key_query_response.block_hash;
        Ok(())
    }

    async fn reset_nonce_and_block_hash(&self) -> Result<()> {
        let access_key_query_response = self.get_access_key_query().await?;

        let QueryResponseKind::AccessKey(access_key) = access_key_query_response.kind else {
            anyhow::bail!(
                "Unexpected query response kind: {:?}",
                access_key_query_response.kind
            );
        };

        self.nonce
            .store(access_key.nonce + 1, std::sync::atomic::Ordering::SeqCst);
        *self.block_hash.write().await = access_key_query_response.block_hash;

        Ok(())
    }

    async fn fetch_add_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    async fn transfer(self: Arc<Self>, token: AccountId, args: Vec<u8>) -> Result<()> {
        let transaction = Transaction::V0(TransactionV0 {
            signer_id: self.signer.account_id.clone(),
            public_key: self.signer.public_key.clone(),
            nonce: self.fetch_add_nonce().await,
            receiver_id: token,
            block_hash: *self.block_hash.read().await,
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ft_transfer_call".to_string(),
                args,
                gas: FT_TRANSFER_CALL_GAS,
                deposit: FT_TRANSFER_CALL_DEPOSIT,
            }))],
        });

        let request = RpcBroadcastTxAsyncRequest {
            signed_transaction: transaction
                .sign(&near_crypto::Signer::InMemory(self.signer.clone())),
        };

        let tx_hash = self.jsonrpc_client.call(request).await?;
        info!("Transaction sent: {:?}", tx_hash);

        Ok(())
    }

    async fn send_transfers(
        self: Arc<Self>,
        payload: Vec<u8>,
        token: AccountId,
        duration: u64,
        reset_block_hash_interval: u64,
    ) -> Result<()> {
        self.reset_nonce_and_block_hash().await?;

        let refresher = {
            let this = self.clone();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(reset_block_hash_interval));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(e) = this.reset_block_hash().await {
                        warn!("block-hash refresh failed: {e:?}");
                    }
                }
            })
        };

        let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let mut inflight = FuturesUnordered::new();
        let start = tokio::time::Instant::now();

        while start.elapsed().as_secs() < duration {
            if inflight.len() >= MAX_IN_FLIGHT {
                let _ = inflight.next().await;
                continue;
            }

            inflight.push(tokio::spawn({
                let semaphore = semaphore.clone();
                let token = token.clone();
                let payload = payload.clone();
                let client = self.clone();

                async move {
                    let Ok(_permit) = semaphore.acquire().await else {
                        warn!("Failed to acquire semaphore permit");
                        return;
                    };

                    if let Err(e) = client.transfer(token, payload).await {
                        warn!("Error sending transfer: {:?}", e);
                    }
                }
            }));
        }

        refresher.abort();

        join_all(inflight).await;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let args = CliArgs::parse();
    let client = Arc::new(Client::new(std::env::var("RPC_URL")?)?);

    let payload = serde_json::json!({
        "amount": args.amount.to_string(),
        "msg": serde_json::json!({
            "recipient": args.recipient,
            "fee": args.fee.to_string(),
            "native_token_fee": "0"
        }).to_string(),
        "receiver_id": args.omni_bridge,
    })
    .to_string()
    .into_bytes();

    client
        .send_transfers(
            payload,
            args.token,
            args.duration,
            args.reset_block_hash_interval,
        )
        .await?;

    Ok(())
}
