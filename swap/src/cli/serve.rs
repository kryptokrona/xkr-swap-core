//! JSON-RPC serve daemon for the taker side of the swap engine.
//!
//! The wallet GUI (aesir, an Electron app) can't use the Tauri command layer
//! (`src-tauri`), so it drives the engine over HTTP JSON-RPC instead. This module
//! wraps an already-built `cli::api::Context` and exposes the taker operations
//! the GUI needs:
//!
//!   * `status`          -- readiness probe.
//!   * `buy_xmr_direct`  -- start a swap against an explicitly-provided maker
//!                          (e.g. the local ASB), skipping the interactive
//!                          maker-selection. Returns the swap id immediately.
//!   * `swap_infos`      -- all swaps and their current state (poll for progress).
//!   * `history`         -- completed-swap history.
//!   * `balance`         -- the taker's Bitcoin balance.
//!   * `resume`          -- resume a swap by id.

use crate::cli::api::Context;
use crate::cli::api::request::{
    BalanceArgs, BuyXmrDirectArgs, GetBitcoinAddressArgs, GetBitcoinTransactionsArgs,
    GetHistoryArgs, GetSellersArgs, GetSwapInfosAllArgs, Request, ResumeSwapArgs, WithdrawBtcArgs,
};
use anyhow::Result;
use jsonrpsee::RpcModule;
use jsonrpsee::server::ServerBuilder;
use jsonrpsee::types::ErrorObjectOwned;
use libp2p::{Multiaddr, PeerId};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Map any error into a JSON-RPC error object.
fn rpc_err(e: impl std::fmt::Display) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
}

#[derive(Deserialize)]
struct BuyXmrDirectParams {
    /// The maker's libp2p multiaddress.
    seller_multiaddr: String,
    /// The maker's libp2p peer id.
    seller_peer_id: String,
    /// The BTC amount to lock, in satoshis.
    btc_amount_sat: u64,
    /// The XKR address to receive the swapped funds at.
    xkr_receive_address: String,
    /// Optional BTC change address (defaults to an internal wallet address).
    #[serde(default)]
    bitcoin_change_address: Option<String>,
}

#[derive(Deserialize)]
struct ResumeParams {
    swap_id: String,
    /// Optional fresh dialable address for the maker. Aesir bridges the maker over
    /// HyperSwarm on an ephemeral local port that dies with the process, so after a
    /// restart the DB's stored address is dead. Supplying the newly-opened bridge
    /// address here re-points the peer before resuming, so the swap can reconnect.
    #[serde(default)]
    seller_multiaddr: Option<String>,
}

#[derive(Deserialize)]
struct WithdrawBtcParams {
    /// Destination Bitcoin address.
    address: String,
    /// Amount in satoshis. Omit / null to drain the wallet.
    #[serde(default)]
    amount_sat: Option<u64>,
}

#[derive(Deserialize)]
struct SwapErrorParams {
    /// The swap to fetch the last recorded failure reason for.
    swap_id: String,
}

/// Serve the taker JSON-RPC API on `host:port` from an already-built `Context`
/// (its p2p event loop is already running). Blocks until the server stops.
/// Keep the Bitcoin wallet balance fresh in the background so the frequently
/// polled `balance` RPC can return the persisted value instantly instead of
/// forcing a full electrum sync (which piled up and timed out the client).
/// Syncs once immediately, then every `SYNC_INTERVAL`. The wallet already
/// persists its state to disk, so a restart serves the last-known balance until
/// the first background sync lands.
fn spawn_background_bitcoin_sync(context: Arc<Context>) {
    // Kept short so incoming (received) BTC txs surface in the GUI quickly --
    // otherwise a deposit can take up to a full interval to appear, which makes
    // swapping feel unsafe. Sent txs already appear immediately (the wallet knows
    // them on broadcast); this interval only bounds how fast we notice deposits.
    const SYNC_INTERVAL: Duration = Duration::from_secs(10);
    tokio::spawn(async move {
        loop {
            match context.try_get_bitcoin_wallet().await {
                Ok(wallet) => {
                    if let Err(error) = wallet.sync().await {
                        tracing::warn!(?error, "Background Bitcoin sync failed");
                    }
                }
                Err(error) => {
                    tracing::debug!(?error, "Bitcoin wallet not ready for background sync yet");
                }
            }
            tokio::time::sleep(SYNC_INTERVAL).await;
        }
    });
}

pub async fn run(context: Arc<Context>, host: String, port: u16) -> Result<()> {
    spawn_background_bitcoin_sync(context.clone());

    let mut module: RpcModule<Arc<Context>> = RpcModule::new(context);

    module.register_async_method("status", |_params, _ctx, _ext| async move {
        Ok::<_, ErrorObjectOwned>(serde_json::json!({ "ready": true }))
    })?;

    module.register_async_method("swap_infos", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetSwapInfosAllArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    // The last recorded failure reason for a swap, if any. A swap that fails
    // during setup never reaches swap_infos (it has no SwapSetupCompleted state),
    // so the GUI polls this by swap_id to learn WHY a just-started swap died.
    module.register_async_method("swap_error", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: SwapErrorParams = params.parse().map_err(rpc_err)?;
        let swap_id = Uuid::from_str(&p.swap_id).map_err(rpc_err)?;
        // (message, terminal): terminal=false means "still trying" (transient),
        // terminal=true means the swap gave up.
        let (error, terminal) = match ctx.get_swap_error(&swap_id) {
            Some((msg, terminal)) => (Some(msg), terminal),
            None => (None, false),
        };
        serde_json::to_value(
            serde_json::json!({ "swap_id": p.swap_id, "error": error, "terminal": terminal }),
        )
        .map_err(rpc_err)
    })?;

    module.register_async_method("history", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetHistoryArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("balance", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        // Return the persisted balance WITHOUT forcing an electrum sync. The UI
        // polls this frequently; a per-call full sync piled up and blew past the
        // client timeout. `spawn_background_bitcoin_sync` keeps the value fresh.
        let r = BalanceArgs { force_refresh: false }
            .request(ctx)
            .await
            .map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("bitcoin_address", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetBitcoinAddressArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("bitcoin_transactions", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetBitcoinTransactionsArgs
            .request(ctx)
            .await
            .map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("list_sellers", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetSellersArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("withdraw_btc", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: WithdrawBtcParams = params.parse().map_err(rpc_err)?;
        let args = WithdrawBtcArgs {
            address: bitcoin::Address::from_str(&p.address)
                .map_err(rpc_err)?
                .assume_checked(),
            amount: p.amount_sat.map(bitcoin::Amount::from_sat),
        };
        let r = args.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("resume", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: ResumeParams = params.parse().map_err(rpc_err)?;
        let swap_id = Uuid::from_str(&p.swap_id).map_err(rpc_err)?;
        // Re-point the maker at the caller-provided (freshly bridged) address before
        // resuming; without this, resume keeps dialing the dead stored port.
        if let Some(addr) = p.seller_multiaddr.as_deref() {
            let multiaddr = Multiaddr::from_str(addr).map_err(rpc_err)?;
            let db = ctx.try_get_db().await.map_err(rpc_err)?;
            if let Ok(peer_id) = db.get_peer_id(swap_id).await {
                db.insert_address(peer_id, multiaddr.clone())
                    .await
                    .map_err(rpc_err)?;
                let mut handle = ctx.try_get_event_loop_handle().await.map_err(rpc_err)?;
                handle
                    .queue_peer_address(peer_id, multiaddr)
                    .await
                    .map_err(rpc_err)?;
            }
        }
        let r = ResumeSwapArgs { swap_id }
            .request(ctx)
            .await
            .map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("buy_xmr_direct", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: BuyXmrDirectParams = params.parse().map_err(rpc_err)?;
        let args = BuyXmrDirectArgs {
            seller_multiaddr: Multiaddr::from_str(&p.seller_multiaddr).map_err(rpc_err)?,
            seller_peer_id: PeerId::from_str(&p.seller_peer_id).map_err(rpc_err)?,
            btc_amount: bitcoin::Amount::from_sat(p.btc_amount_sat),
            xkr_receive_address: p.xkr_receive_address,
            bitcoin_change_address: match p.bitcoin_change_address {
                Some(s) => Some(bitcoin::Address::from_str(&s).map_err(rpc_err)?),
                None => None,
            },
        };
        let r = args.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    let server = ServerBuilder::default()
        .build((host.as_str(), port))
        .await?;
    tracing::info!("XKR swap serve daemon listening on {host}:{port}");
    let handle = server.start(module);
    handle.stopped().await;
    Ok(())
}
