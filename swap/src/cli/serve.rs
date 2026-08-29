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
    BalanceArgs, BuyXmrDirectArgs, GetHistoryArgs, GetSwapInfosAllArgs, Request, ResumeSwapArgs,
};
use anyhow::Result;
use jsonrpsee::RpcModule;
use jsonrpsee::server::ServerBuilder;
use jsonrpsee::types::ErrorObjectOwned;
use libp2p::{Multiaddr, PeerId};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
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
}

/// Serve the taker JSON-RPC API on `host:port` from an already-built `Context`
/// (its p2p event loop is already running). Blocks until the server stops.
pub async fn run(context: Arc<Context>, host: String, port: u16) -> Result<()> {
    let mut module: RpcModule<Arc<Context>> = RpcModule::new(context);

    module.register_async_method("status", |_params, _ctx, _ext| async move {
        Ok::<_, ErrorObjectOwned>(serde_json::json!({ "ready": true }))
    })?;

    module.register_async_method("swap_infos", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetSwapInfosAllArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("history", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = GetHistoryArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("balance", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = BalanceArgs { force_refresh: true }
            .request(ctx)
            .await
            .map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("resume", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: ResumeParams = params.parse().map_err(rpc_err)?;
        let swap_id = Uuid::from_str(&p.swap_id).map_err(rpc_err)?;
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
