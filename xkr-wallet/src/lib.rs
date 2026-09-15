use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct XkrWalletClient {
    base_url: String,
    http: reqwest::Client,
}

impl XkrWalletClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

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

    pub async fn ping(&self) -> Result<String> {
        Ok(self
            .call("ping", json!({}))
            .await?
            .as_str()
            .unwrap_or_default()
            .to_string())
    }

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
