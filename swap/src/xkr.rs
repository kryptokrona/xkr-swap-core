//! XKR (Kryptokrona) wallet adapter for the swap engine.
//!
//! Bridges the engine's ed25519 key material to the XKR wallet JSON-RPC service
//! (`xkr-wallet-rpc.cjs`, backed by kryptokrona-wallet-backend-js) via the
//! `xkr-wallet` client crate. This is where the BTC<->XMR engine becomes a
//! BTC<->XKR engine on the "Monero side": the swap protocol, the Bitcoin side,
//! and the cross-curve DLEQ / adaptor-signature crypto are unchanged, because
//! XKR shares Monero's ed25519 curve and one-time-address / key-image model.
//!
//! The shared 2-of-2 output is spent with an ordinary transaction once both
//! spend shares are known. The combined spend key the engine computes for the
//! Monero redeem — `State5::xmr_keys().0` (i.e. `s_a + s_b`) — is a curve25519
//! scalar whose canonical little-endian bytes are exactly the XKR private spend
//! key; likewise the shared view key. So the mapping is just: take the 32-byte
//! scalar, hex-encode it, and hand it to the XKR wallet service.

use anyhow::{Context, Result, anyhow};
use xkr_wallet::XkrWalletClient;

/// Default endpoint of the in-wallet XKR JSON-RPC service.
const DEFAULT_RPC_URL: &str = "http://127.0.0.1:40000";

/// Convert an agreed swap amount into XKR atomic units.
///
/// The engine models the "other coin" amount internally as a Monero [`Amount`]
/// (12 decimals — piconero). XKR (Kryptokrona) uses 5 decimals, so the value the
/// XKR wallet RPC service expects is scaled down by the 7-decimal difference.
/// Both parties call this on the same agreed `Amount`, and the truncation is
/// deterministic, so Alice's lock and Bob's watch always agree on the integer.
///
/// The BTC<->XKR *rate* is a separate concern (the maker's price feed decides how
/// many coins a given BTC amount buys); this only fixes the decimal scaling so a
/// given coin amount maps to the right number of XKR atomic units.
pub fn to_xkr_atomic(amount: crate::monero::Amount) -> u64 {
    const MONERO_DECIMALS: u32 = 12;
    const XKR_DECIMALS: u32 = 5;
    const SCALE: u64 = 10u64.pow(MONERO_DECIMALS - XKR_DECIMALS); // 1e7
    amount.as_pico() / SCALE
}

/// Adapter over the XKR wallet JSON-RPC service, speaking the engine's types.
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

    /// Build from the `XKR_WALLET_RPC_URL` env var, defaulting to localhost.
    pub fn from_env() -> Self {
        let url = std::env::var("XKR_WALLET_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
        Self::new(url)
    }

    fn hex(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Decode a 64-char lower/upper hex string into 32 bytes.
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

    /// The ASB's own XKR wallet keys `(spend, view)` used to fund locks, read from
    /// `XKR_ASB_SPEND_SECRET` / `XKR_ASB_VIEW_SECRET` (64-char hex).
    /// TODO: source these from ASB config instead of env.
    pub fn asb_keys_from_env() -> Result<([u8; 32], [u8; 32])> {
        let spend = Self::unhex(
            &std::env::var("XKR_ASB_SPEND_SECRET").context("XKR_ASB_SPEND_SECRET not set")?,
        )?;
        let view = Self::unhex(
            &std::env::var("XKR_ASB_VIEW_SECRET").context("XKR_ASB_VIEW_SECRET not set")?,
        )?;
        Ok((spend, view))
    }

    /// Encode the shared 2-of-2 output keys (`B_A+B_B`, `V_A+V_B`) as a fundable
    /// XKR address — the address the XKR provider locks funds into.
    pub async fn shared_address(
        &self,
        spend_public_key: [u8; 32],
        view_public_key: [u8; 32],
    ) -> Result<String> {
        self.client
            .encode_address(&Self::hex(spend_public_key), &Self::hex(view_public_key))
            .await
    }

    /// Block until the counterparty's XKR lock lands at the shared address.
    /// Returns the hash of the detected lock deposit.
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

    /// Redeem: reconstruct the shared wallet from the combined `(spend, view)`
    /// secrets and sweep the locked output to `dest`. The XKR analogue of
    /// `State5::construct_xmr_redeem_transaction`. Returns the sweep tx hash.
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

    /// Alice's side: send `amount` from the ASB's own wallet to the shared
    /// `dest` address (the XKR lock). Returns the broadcast tx hash.
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

    /// Block until a redeem/refund sweep (a spend from the shared output) reaches
    /// `confirmations` depth. Keyed by `tx_hash`, so a swap that crashed after
    /// broadcasting can resume the confirm without re-sweeping. The XKR analogue
    /// of waiting on `monero_wallet` for the redeem transaction to confirm.
    /// Returns the observed confirmation depth.
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
        // A private spend key derived from `State5::xmr_keys().0.to_bytes()`
        // must hex-encode to the same 64-char lower-hex the XKR wallet service
        // (and swap_spike's COMBINED_SPEND_KEY) uses.
        let bytes = [0x0eu8; 32];
        let h = XkrWallet::hex(bytes);
        assert_eq!(h.len(), 64);
        assert_eq!(&h[..4], "0e0e");
    }

    #[test]
    fn xkr_atomic_scaling_drops_seven_decimals() {
        use crate::monero::Amount;
        // 1 whole coin: 1e12 pico -> 1e5 XKR atomic.
        assert_eq!(to_xkr_atomic(Amount::from_pico(1_000_000_000_000)), 100_000);
        // 0.0025 coin (the CI swap amount for 2500 sat @ FixedRate 0.01).
        assert_eq!(to_xkr_atomic(Amount::from_pico(2_500_000_000)), 250);
        // Sub-atomic-XKR remainders truncate deterministically (so both parties agree).
        assert_eq!(to_xkr_atomic(Amount::from_pico(9_999_999)), 0);
        assert_eq!(to_xkr_atomic(Amount::from_pico(10_000_000)), 1);
    }
}
