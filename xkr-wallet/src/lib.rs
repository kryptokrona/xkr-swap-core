//! XKR (Kryptokrona) wallet client for BTC<->XKR atomic swaps.
//!
//! This is the Kryptokrona side of the swap. The swap engine owns the protocol,
//! the Bitcoin side, and the cross-curve DLEQ / adaptor-signature crypto; it
//! computes the shared 2-of-2 ed25519 keys (spend pubkey `B_A+B_B`, view secret
//! `v_A+v_B`) and then calls the methods below. Everything that touches the
//! Kryptokrona chain lives behind this boundary, in a small JSON-RPC service
//! (backed by kryptokrona-wallet-backend-js) that ships inside the wallet app.
//!
//! Keeping the XKR wallet behind an RPC boundary means the engine needs no
//! Kryptokrona consensus code: the shared output is spent with an ordinary
//! transaction once both spend shares are known.

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

/// A client for the XKR wallet JSON-RPC service.
#[derive(Clone)]
pub struct XkrWalletClient {
    base_url: String,
    http: reqwest::Client,
}

impl XkrWalletClient {
    /// Create a client targeting the service base URL, e.g. `http://127.0.0.1:40000`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Issue a single JSON-RPC 2.0 call and return its `result`.
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let response = self
            .http
            .post(&self.base_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("xkr-wallet-rpc request failed: {method}"))?;
        let value: Value = response
            .json()
            .await
            .context("xkr-wallet-rpc returned invalid JSON")?;
        if let Some(error) = value.get("error") {
            return Err(anyhow!("xkr-wallet-rpc error for {method}: {error}"));
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("xkr-wallet-rpc returned no result for {method}"))
    }

    /// Health check. Returns the service's `pong`.
    pub async fn ping(&self) -> Result<String> {
        Ok(self
            .call("ping", json!({}))
            .await?
            .as_str()
            .unwrap_or_default()
            .to_string())
    }

    /// Encode the shared 2-of-2 keys as a fundable Kryptokrona address.
    pub async fn encode_address(
        &self,
        spend_public_key: &str,
        view_public_key: &str,
    ) -> Result<String> {
        let result = self
            .call(
                "encodeAddress",
                json!({ "spendPublicKey": spend_public_key, "viewPublicKey": view_public_key }),
            )
            .await?;
        result
            .get("address")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("encodeAddress returned no address"))
    }

    /// Block until the locked deposit lands at `address` (watched view-only).
    /// Returns the hash of the detected lock deposit.
    pub async fn watch_for_lock(
        &self,
        address: &str,
        view_secret: &str,
        amount: u64,
        timeout_ms: Option<u64>,
    ) -> Result<String> {
        let mut params =
            json!({ "address": address, "viewSecret": view_secret, "amount": amount });
        if let Some(timeout_ms) = timeout_ms {
            params["timeoutMs"] = json!(timeout_ms);
        }
        let result = self.call("watchForLock", params).await?;
        if result.get("detected").and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!("watchForLock did not detect the deposit"));
        }
        result
            .get("txHash")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("watchForLock detected the deposit but returned no txHash"))
    }

    /// Reconstruct the shared wallet from the combined secrets and sweep to `dest`.
    /// Returns the sweep transaction hash.
    pub async fn sweep(
        &self,
        spend_secret: &str,
        view_secret: &str,
        dest: &str,
        fee: Option<u64>,
    ) -> Result<String> {
        let mut params =
            json!({ "spendSecret": spend_secret, "viewSecret": view_secret, "destAddress": dest });
        if let Some(fee) = fee {
            params["fee"] = json!(fee);
        }
        let result = self.call("sweep", params).await?;
        result
            .get("txHash")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("sweep returned no txHash"))
    }

    /// Poll until a transaction spending from the shared address (redeem/refund)
    /// reaches `confirmations` depth. Keyed by `tx_hash`, so it is safe to re-call
    /// after a restart without re-broadcasting. Returns the observed depth.
    pub async fn confirm_tx(
        &self,
        spend_secret: &str,
        view_secret: &str,
        tx_hash: &str,
        confirmations: Option<u64>,
        timeout_ms: Option<u64>,
    ) -> Result<u64> {
        let mut params = json!({
            "spendSecret": spend_secret,
            "viewSecret": view_secret,
            "txHash": tx_hash,
        });
        if let Some(confirmations) = confirmations {
            params["confirmations"] = json!(confirmations);
        }
        if let Some(timeout_ms) = timeout_ms {
            params["timeoutMs"] = json!(timeout_ms);
        }
        let result = self.call("confirmTx", params).await?;
        result
            .get("confirmations")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("confirmTx returned no confirmations"))
    }

    /// Alice's side: send `amount` from the sender's own wallet to `dest` (the
    /// shared address — the XKR lock). Returns the broadcast tx hash.
    pub async fn lock_send(
        &self,
        sender_spend_secret: &str,
        sender_view_secret: &str,
        dest: &str,
        amount: u64,
        fee: Option<u64>,
    ) -> Result<String> {
        let mut params = json!({
            "senderSpendSecret": sender_spend_secret,
            "senderViewSecret": sender_view_secret,
            "destAddress": dest,
            "amount": amount,
        });
        if let Some(fee) = fee {
            params["fee"] = json!(fee);
        }
        let result = self.call("lockSend", params).await?;
        result
            .get("txHash")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("lockSend returned no txHash"))
    }

    /// Unlocked (spendable) and locked balance of the wallet reconstructed from
    /// the given secrets, in XKR atomic units. Used by the maker to bound quotes
    /// and gate swap setup against its real XKR funding.
    pub async fn balance(
        &self,
        spend_secret: &str,
        view_secret: &str,
        scan_height: Option<u64>,
    ) -> Result<(u64, u64)> {
        let mut params = json!({ "spendSecret": spend_secret, "viewSecret": view_secret });
        if let Some(scan_height) = scan_height {
            params["scanHeight"] = json!(scan_height);
        }
        let result = self.call("balance", params).await?;
        let unlocked = result
            .get("unlocked")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("balance returned no unlocked amount"))?;
        let locked = result.get("locked").and_then(Value::as_u64).unwrap_or(0);
        Ok((unlocked, locked))
    }
}
