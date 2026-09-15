use crate::cli::api::Context;
use crate::cli::api::request::{
    BalanceArgs, BuyXmrDirectArgs, CancelAndRefundArgs, GetBitcoinAddressArgs,
    GetBitcoinTransactionsArgs, GetHistoryArgs, GetSellersArgs, GetSwapInfosAllArgs, Request,
    ResumeSwapArgs, SuspendCurrentSwapArgs, WithdrawBtcArgs,
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

fn rpc_err(e: impl std::fmt::Display) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
}

#[derive(Deserialize)]
struct BuyXmrDirectParams {
    seller_multiaddr: String,
    seller_peer_id: String,
    btc_amount_sat: u64,
    xkr_receive_address: String,
    #[serde(default)]
    bitcoin_change_address: Option<String>,
}

#[derive(Deserialize)]
struct ResumeParams {
    swap_id: String,
    #[serde(default)]
    seller_multiaddr: Option<String>,
}

#[derive(Deserialize)]
struct CancelParams {
    swap_id: String,
}

#[derive(Deserialize)]
struct WithdrawBtcParams {
    address: String,
    #[serde(default)]
    amount_sat: Option<u64>,
}

#[derive(Deserialize)]
struct SwapErrorParams {
    swap_id: String,
}

#[derive(Deserialize)]
struct EstimateLockFeeParams {
    btc_amount_sat: u64,
}

fn spawn_background_bitcoin_sync(context: Arc<Context>) {
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

    module.register_async_method("estimate_lock_fee", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: EstimateLockFeeParams = params.parse().map_err(rpc_err)?;
        let wallet = ctx.try_get_bitcoin_wallet().await.map_err(rpc_err)?;
        let amount = bitcoin::Amount::from_sat(p.btc_amount_sat);
        let fee = wallet
            .estimate_fee(swap_core::bitcoin::TxLock::weight(), Some(amount))
            .await
            .map_err(rpc_err)?;
        serde_json::to_value(serde_json::json!({ "fee_sat": fee.to_sat() })).map_err(rpc_err)
    })?;

    module.register_async_method("swap_error", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: SwapErrorParams = params.parse().map_err(rpc_err)?;
        let swap_id = Uuid::from_str(&p.swap_id).map_err(rpc_err)?;
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

    module.register_async_method("suspend_current_swap", |_params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let r = SuspendCurrentSwapArgs.request(ctx).await.map_err(rpc_err)?;
        serde_json::to_value(r).map_err(rpc_err)
    })?;

    module.register_async_method("cancel_and_refund", |params, ctx, _ext| async move {
        let ctx: Arc<Context> = (*ctx).clone();
        let p: CancelParams = params.parse().map_err(rpc_err)?;
        let swap_id = Uuid::from_str(&p.swap_id).map_err(rpc_err)?;
        let r = CancelAndRefundArgs { swap_id }
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
