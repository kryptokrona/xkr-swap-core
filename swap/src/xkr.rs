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

use anyhow::Result;
use xkr_wallet::XkrWalletClient;

/// Default endpoint of the in-wallet XKR JSON-RPC service.
const DEFAULT_RPC_URL: &str = "http://127.0.0.1:40000";

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
    pub async fn watch_for_lock(
        &self,
        address: &str,
        view_secret: [u8; 32],
        amount: u64,
        timeout_ms: Option<u64>,
    ) -> Result<()> {
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
}
