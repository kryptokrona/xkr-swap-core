use anyhow::{Context, Result, anyhow};
use xkr_wallet::XkrWalletClient;

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:40000";

pub fn to_xkr_atomic(amount: crate::monero::Amount) -> u64 {
    const MONERO_DECIMALS: u32 = 12;
    const XKR_DECIMALS: u32 = 5;
    const SCALE: u64 = 10u64.pow(MONERO_DECIMALS - XKR_DECIMALS); // 1e7
    amount.as_pico() / SCALE
}

pub fn from_xkr_atomic(atomic: u64) -> crate::monero::Amount {
    const MONERO_DECIMALS: u32 = 12;
    const XKR_DECIMALS: u32 = 5;
    const SCALE: u64 = 10u64.pow(MONERO_DECIMALS - XKR_DECIMALS); // 1e7
    crate::monero::Amount::from_pico(atomic.saturating_mul(SCALE))
}

#[derive(Clone)]
pub struct XkrWallet {
    client: XkrWalletClient,
}

impl XkrWallet {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: XkrWalletClient::new(base_url),
        }
    }

    pub fn from_env() -> Self {
        let url = std::env::var("XKR_WALLET_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
        Self::new(url)
    }

    fn hex(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn unhex(s: &str) -> Result<[u8; 32]> {
        let s = s.trim();
        if s.len() != 64 {
            return Err(anyhow!("expected 64 hex chars, got {}", s.len()));
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk)?, 16)
                .context("invalid hex in key")?;
        }
        Ok(out)
    }

    pub fn asb_keys_from_env() -> Result<([u8; 32], [u8; 32])> {
        let spend = Self::unhex(
            &std::env::var("XKR_ASB_SPEND_SECRET").context("XKR_ASB_SPEND_SECRET not set")?,
        )?;
        let view = Self::unhex(
            &std::env::var("XKR_ASB_VIEW_SECRET").context("XKR_ASB_VIEW_SECRET not set")?,
        )?;
        Ok((spend, view))
    }

    pub async fn unlocked_balance(
        &self,
        spend_secret: [u8; 32],
        view_secret: [u8; 32],
    ) -> Result<crate::monero::Amount> {
        let (unlocked, _locked) = self
            .client
            .balance(&Self::hex(spend_secret), &Self::hex(view_secret), None)
            .await?;
        Ok(from_xkr_atomic(unlocked))
    }

    pub async fn shared_address(
        &self,
        spend_public_key: [u8; 32],
        view_public_key: [u8; 32],
    ) -> Result<String> {
        self.client
            .encode_address(&Self::hex(spend_public_key), &Self::hex(view_public_key))
            .await
    }

    pub async fn watch_for_lock(
        &self,
        address: &str,
        view_secret: [u8; 32],
        amount: u64,
        timeout_ms: Option<u64>,
    ) -> Result<String> {
        self.client
            .watch_for_lock(address, &Self::hex(view_secret), amount, timeout_ms)
            .await
    }

    pub async fn redeem(
        &self,
        spend_secret: [u8; 32],
        view_secret: [u8; 32],
        dest: &str,
        fee: Option<u64>,
    ) -> Result<String> {
        self.client
            .sweep(&Self::hex(spend_secret), &Self::hex(view_secret), dest, fee)
            .await
    }

    pub async fn lock_send(
        &self,
        sender_spend_secret: [u8; 32],
        sender_view_secret: [u8; 32],
        dest: &str,
        amount: u64,
        fee: Option<u64>,
    ) -> Result<String> {
        self.client
            .lock_send(
                &Self::hex(sender_spend_secret),
                &Self::hex(sender_view_secret),
                dest,
                amount,
                fee,
            )
            .await
    }

    pub async fn wait_until_confirmed(
        &self,
        spend_secret: [u8; 32],
        view_secret: [u8; 32],
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<u64> {
        self.client
            .confirm_tx(
                &Self::hex(spend_secret),
                &Self::hex(view_secret),
                tx_hash,
                Some(confirmations),
                None,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_of_scalar_bytes_is_canonical_64_char() {
        let bytes = [0x0eu8; 32];
        let h = XkrWallet::hex(bytes);
        assert_eq!(h.len(), 64);
        assert_eq!(&h[..4], "0e0e");
    }

    #[test]
    fn xkr_atomic_scaling_drops_seven_decimals() {
        use crate::monero::Amount;
        assert_eq!(to_xkr_atomic(Amount::from_pico(1_000_000_000_000)), 100_000);
        assert_eq!(to_xkr_atomic(Amount::from_pico(2_500_000_000)), 250);
        assert_eq!(to_xkr_atomic(Amount::from_pico(9_999_999)), 0);
        assert_eq!(to_xkr_atomic(Amount::from_pico(10_000_000)), 1);
    }
}
