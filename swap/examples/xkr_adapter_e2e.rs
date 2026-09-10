//! Level-2 end-to-end driver for the XKR wallet adapter.
//!
//! Unlike the pure-service smoke test (which drives `xkr-wallet-rpc.cjs` with
//! curl), this runs the engine's *own* boundary code — `swap::xkr::XkrWallet`
//! (and, through it, the `xkr-wallet` client) — against a live XKR wallet RPC
//! service. These are the exact calls the ported Bob/Alice state-machine arms
//! make: `asb_keys_from_env` + `lock_send` (Alice's lock), `shared_address` +
//! `watch_for_lock` (Bob's lock detection), `redeem` (Bob redeem / Alice refund),
//! and `wait_until_confirmed`.
//!
//! On-chain funding and block production are handled by the harness that invokes
//! this (it mines in the background), so the blocking polls here resolve as the
//! chain advances.
//!
//! Config (all via env):
//!   XKR_WALLET_RPC_URL            service URL (default http://127.0.0.1:40000)
//!   XKR_ASB_SPEND_SECRET/VIEW_SECRET  party A's keys (used by asb_keys_from_env)
//!   SHARED_SPEND_PUB / SHARED_VIEW_PUB  shared 2-of-2 public keys (hex)
//!   COMBINED_SPEND / COMBINED_VIEW      combined shared secrets (hex)
//!   SHARED_ADDR                   expected shared address
//!   FUNDER_ADDR                   sweep destination
//!   LOCK_AMOUNT / FEE             atomic units

use anyhow::{Context, Result, anyhow, bail};
use swap::xkr::XkrWallet;

fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing env var {key}"))
}

/// Decode a 64-char hex string to 32 bytes — mirrors what the engine feeds the
/// adapter (which is `scalar.as_bytes()`; swap_spike prints the same bytes hex).
fn unhex(s: &str) -> Result<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 {
        bail!("expected 64 hex chars, got {}", s.len());
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk)?, 16)?;
    }
    Ok(out)
}

fn step(n: u32, msg: &str) {
    println!("[engine-e2e] step {n}: {msg}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let shared_addr = env("SHARED_ADDR")?;
    let funder_addr = env("FUNDER_ADDR")?;
    let lock_amount: u64 = env("LOCK_AMOUNT")?.parse().context("LOCK_AMOUNT")?;
    let fee: u64 = env("FEE")?.parse().context("FEE")?;
    let shared_spend_pub = unhex(&env("SHARED_SPEND_PUB")?)?;
    let shared_view_pub = unhex(&env("SHARED_VIEW_PUB")?)?;
    let combined_spend = unhex(&env("COMBINED_SPEND")?)?;
    let combined_view = unhex(&env("COMBINED_VIEW")?)?;

    // The adapter reads XKR_WALLET_RPC_URL, exactly as the engine does.
    let xkr = XkrWallet::from_env();

    // 1. shared_address: the adapter's hex-encode + encodeAddress must reproduce
    //    the shared 2-of-2 address.
    step(1, "shared_address()");
    let derived = xkr
        .shared_address(shared_spend_pub, shared_view_pub)
        .await
        .context("shared_address failed")?;
    if derived != shared_addr {
        return Err(anyhow!(
            "shared_address mismatch: adapter={derived} expected={shared_addr}"
        ));
    }
    println!("[engine-e2e]   OK shared_address == {derived}");

    // 2. Alice's lock path: asb_keys_from_env() + lock_send().
    step(2, "asb_keys_from_env() + lock_send()");
    let (asb_spend, asb_view) =
        XkrWallet::asb_keys_from_env().context("asb_keys_from_env failed")?;
    let lock_txid = xkr
        .lock_send(asb_spend, asb_view, &shared_addr, lock_amount, Some(fee))
        .await
        .context("lock_send failed")?;
    println!("[engine-e2e]   OK lock_send txid = {lock_txid}");

    // 3. Bob's lock detection: watch_for_lock() (view-only scan), must see the
    //    same tx the lock produced.
    step(3, "watch_for_lock()");
    let watch_txid = xkr
        .watch_for_lock(&shared_addr, combined_view, lock_amount, Some(180_000))
        .await
        .context("watch_for_lock failed")?;
    if watch_txid != lock_txid {
        return Err(anyhow!(
            "watch_for_lock txid {watch_txid} != lock txid {lock_txid}"
        ));
    }
    println!("[engine-e2e]   OK watch_for_lock detected {watch_txid}");

    // 4. Bob redeem / Alice refund path: redeem() sweeps the shared output.
    step(4, "redeem() (sweep shared output)");
    let sweep_txid = xkr
        .redeem(combined_spend, combined_view, &funder_addr, Some(fee))
        .await
        .context("redeem failed")?;
    println!("[engine-e2e]   OK redeem txid = {sweep_txid}");

    // 5. wait_until_confirmed(): confirm the sweep on-chain.
    step(5, "wait_until_confirmed()");
    let confirmations = xkr
        .wait_until_confirmed(combined_spend, combined_view, &sweep_txid, 1)
        .await
        .context("wait_until_confirmed failed")?;
    println!("[engine-e2e]   OK sweep confirmed (depth {confirmations})");

    println!("[engine-e2e] ENGINE ADAPTER E2E PASSED");
    Ok(())
}
