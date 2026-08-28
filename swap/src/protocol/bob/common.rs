use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use uuid::Uuid;

use bitcoin_wallet;
use swap_machine::bob::{State3, State4, State5};

use crate::cli::SwapEventLoopHandle;
use crate::common::retry;
use crate::monero;
use crate::xkr::XkrWallet;
use crate::monero::MoneroAddressPool;
use monero_interface::PublishTransaction;

pub(super) trait XmrRedeemable {
    /// Sweep the shared 2-of-2 XKR output to `xkr_receive_address`, returning the
    /// broadcast tx hash. The XKR analogue of constructing+publishing the Monero
    /// redeem: the wallet `sweep` builds, signs and broadcasts atomically, so there
    /// is no separate publish step and no persisted transaction object.
    async fn sweep_xmr_redeem(
        self,
        xkr: &XkrWallet,
        swap_id: Uuid,
        xkr_receive_address: &str,
    ) -> Result<String>;
}

pub(super) trait InfallibleXmrRedeemable {
    async fn infallible_sweep_xmr_redeem(
        &self,
        xkr: &XkrWallet,
        swap_id: Uuid,
        xkr_receive_address: &str,
    ) -> String;
}

impl XmrRedeemable for State5 {
    async fn sweep_xmr_redeem(
        self: State5,
        xkr: &XkrWallet,
        swap_id: Uuid,
        xkr_receive_address: &str,
    ) -> Result<String> {
        let (spend_key, view_key) = self.xmr_keys();
        // Canonical little-endian scalar bytes == the XKR private spend/view keys.
        let spend_secret = spend_key.as_bytes();
        let view_secret = view_key.0.as_bytes();

        tracing::info!(%swap_id, dest = %xkr_receive_address, "Sweeping shared XKR output to receive address");

        // Idempotent in the service: a re-sweep after a crashed-but-broadcast attempt
        // returns the existing tx hash instead of double-spending.
        let txid = xkr
            .redeem(spend_secret, view_secret, xkr_receive_address, None)
            .await
            .context("Failed to sweep shared XKR redeem output")?;

        tracing::info!(%swap_id, %txid, "Broadcast XKR redeem sweep");

        Ok(txid)
    }
}

impl InfallibleXmrRedeemable for State5 {
    async fn infallible_sweep_xmr_redeem(
        &self,
        xkr: &XkrWallet,
        swap_id: Uuid,
        xkr_receive_address: &str,
    ) -> String {
        let state_for_retry = self.clone();

        retry(
            "Sweeping XKR redeem",
            || {
                let state = state_for_retry.clone();

                async move {
                    state
                        .sweep_xmr_redeem(xkr, swap_id, xkr_receive_address)
                        .await
                        .map_err(backoff::Error::transient)
                }
            },
            None,
            None,
        )
        .await
        .expect("we never stop retrying to sweep XKR redeem")
    }
}

pub(super) trait WaitForIncomingXmrLockTransaction {
    async fn wait_for_incoming_xmr_lock_transaction(&self, swap_id: Uuid) -> monero::TxHash;
}

impl WaitForIncomingXmrLockTransaction for State3 {
    async fn wait_for_incoming_xmr_lock_transaction(&self, _swap_id: Uuid) -> monero::TxHash {
        let (public_spend_key, private_view_key) = self.xmr_view_keys();
        // Shared 2-of-2 output keys: watch the shared XKR address with the shared
        // view secret until Alice's lock lands, then record its tx hash.
        let spend_public = public_spend_key.as_bytes();
        let view_public = private_view_key.public().0.as_bytes();
        let view_secret = private_view_key.0.as_bytes();
        let amount = self.xmr_amount().as_pico();
        let xkr = XkrWallet::from_env();

        retry(
            "Waiting for incoming XKR lock transaction",
            || {
                let xkr = xkr.clone();
                async move {
                    let address = xkr
                        .shared_address(spend_public, view_public)
                        .await
                        .map_err(backoff::Error::transient)?;
                    let txid = xkr
                        .watch_for_lock(&address, view_secret, amount, None)
                        .await
                        .map_err(backoff::Error::transient)?;
                    Ok(monero::TxHash(txid))
                }
            },
            None,
            None,
        )
        .await
        .expect("we never stop retrying to wait for incoming XKR lock transaction")
    }
}

/// Outcome of validating an XKR lock transaction candidate.
#[derive(Clone, Copy)]
pub(super) enum XmrLockTransactionValidity {
    Invalid,
    Valid,
}

pub(super) trait VerifyXmrLockTransaction {
    async fn verify_xmr_lock_transaction(
        &self,
        tx_hash: monero::TxHash,
    ) -> Result<XmrLockTransactionValidity>;
}

impl VerifyXmrLockTransaction for State3 {
    async fn verify_xmr_lock_transaction(
        &self,
        _tx_hash: monero::TxHash,
    ) -> Result<XmrLockTransactionValidity> {
        let (public_spend_key, private_view_key) = self.xmr_view_keys();
        let spend_public = public_spend_key.as_bytes();
        let view_public = private_view_key.public().0.as_bytes();
        let view_secret = private_view_key.0.as_bytes();
        let amount = self.xmr_amount().as_pico();

        let xkr = XkrWallet::from_env();
        let address = xkr.shared_address(spend_public, view_public).await?;

        // The lock is valid once the shared address has received at least the
        // agreed amount. Hermes funding is dropped in the XKR port (single-dest),
        // so there is never a hermes amount. A short watch returns immediately if
        // the deposit is already present; otherwise it waits briefly for it.
        xkr.watch_for_lock(&address, view_secret, amount, Some(60_000))
            .await
            .context("Failed to observe the XKR lock at the shared address")?;

        Ok(XmrLockTransactionValidity::Valid)
    }
}

pub(super) trait InfallibleVerifyXmrLockTransaction {
    async fn infallible_verify_xmr_lock_transaction(
        self,
        tx_hash: monero::TxHash,
    ) -> XmrLockTransactionValidity;
}

impl<T> InfallibleVerifyXmrLockTransaction for T
where
    T: VerifyXmrLockTransaction + Clone,
{
    async fn infallible_verify_xmr_lock_transaction(
        self,
        tx_hash: monero::TxHash,
    ) -> XmrLockTransactionValidity {
        let state_for_retry = self;

        retry(
            "Verifying XKR lock transaction",
            || {
                let state = state_for_retry.clone();
                let tx_hash = tx_hash.clone();

                async move {
                    state
                        .verify_xmr_lock_transaction(tx_hash)
                        .await
                        .map_err(backoff::Error::transient)
                }
            },
            None,
            None,
        )
        .await
        .expect("we never stop retrying to verify XKR lock transaction")
    }
}

/// Observe the shared XKR lock at its address (view-only). NOTE: the XKR port
/// treats "observed at the shared address with the agreed amount" as confirmed;
/// it does not wait for a Monero-style deep-reorg confirmation window.
async fn watch_shared_lock(
    spend_public: [u8; 32],
    view_public: [u8; 32],
    view_secret: [u8; 32],
    amount: u64,
) -> Result<()> {
    let xkr = XkrWallet::from_env();
    let address = xkr.shared_address(spend_public, view_public).await?;
    xkr.watch_for_lock(&address, view_secret, amount, None)
        .await
        .map(|_txid| ())
}

pub(super) trait WaitForXmrLockTransactionConfirmation {
    async fn infallible_wait_for_xmr_lock_confirmation(
        &self,
        tx_hash: monero::TxHash,
        confirmation_target: u64,
        on_confirmation_update: Option<
            impl Fn((monero::TxHash, u64, u64)) + Send + Clone + 'static,
        >,
    ) -> Result<bool>;
}

impl WaitForXmrLockTransactionConfirmation for State3 {
    async fn infallible_wait_for_xmr_lock_confirmation(
        &self,
        tx_hash: monero::TxHash,
        confirmation_target: u64,
        on_confirmation_update: Option<
            impl Fn((monero::TxHash, u64, u64)) + Send + Clone + 'static,
        >,
    ) -> Result<bool> {
        let (public_spend_key, private_view_key) = self.xmr_view_keys();
        let spend_public = public_spend_key.as_bytes();
        let view_public = private_view_key.public().0.as_bytes();
        let view_secret = private_view_key.0.as_bytes();
        let amount = self.xmr_amount().as_pico();

        retry(
            "Waiting for XKR lock transaction confirmation",
            || async move {
                watch_shared_lock(spend_public, view_public, view_secret, amount)
                    .await
                    .map_err(backoff::Error::transient)
            },
            None,
            None,
        )
        .await?;

        if let Some(cb) = on_confirmation_update {
            cb((tx_hash, confirmation_target, confirmation_target));
        }
        Ok(true)
    }
}

impl WaitForXmrLockTransactionConfirmation for State5 {
    async fn infallible_wait_for_xmr_lock_confirmation(
        &self,
        tx_hash: monero::TxHash,
        confirmation_target: u64,
        on_confirmation_update: Option<
            impl Fn((monero::TxHash, u64, u64)) + Send + Clone + 'static,
        >,
    ) -> Result<bool> {
        let (spend_secret, private_view_key) = self.xmr_keys();
        let spend_public =
            monero_oxide_ext::PublicKey::from_private_key(&spend_secret).as_bytes();
        let view_public = private_view_key.public().0.as_bytes();
        let view_secret = private_view_key.0.as_bytes();
        let amount = self.xmr_amount().as_pico();

        retry(
            "Waiting for XKR lock transaction confirmation",
            || async move {
                watch_shared_lock(spend_public, view_public, view_secret, amount)
                    .await
                    .map_err(backoff::Error::transient)
            },
            None,
            None,
        )
        .await?;

        if let Some(cb) = on_confirmation_update {
            cb((tx_hash, confirmation_target, confirmation_target));
        }
        Ok(true)
    }
}

pub(super) trait WaitForBtcRedeem {
    async fn infallible_wait_for_btc_redeem(
        &self,
        bitcoin_wallet: &dyn bitcoin_wallet::BitcoinWallet,
        force_lookup_interval_secs: u64,
    ) -> State5;
}

impl WaitForBtcRedeem for State4 {
    async fn infallible_wait_for_btc_redeem(
        &self,
        bitcoin_wallet: &dyn bitcoin_wallet::BitcoinWallet,
        force_lookup_interval_secs: u64,
    ) -> State5 {
        let force_lookup_interval = Duration::from_secs(force_lookup_interval_secs);

        let watch_for_redeem = retry(
            "Watching for Bitcoin redeem transaction",
            || {
                let state = self.clone();

                async move {
                    state
                        .watch_for_redeem_btc(bitcoin_wallet)
                        .await
                        .context("Failed to watch for Bitcoin redeem transaction")
                        .map_err(backoff::Error::transient)
                }
            },
            None,
            None,
        );

        let force_lookup = async {
            loop {
                if let Some(state5) = retry(
                    "Checking for Bitcoin redeem transaction",
                    || {
                        let state = self.clone();

                        async move {
                            state
                                .check_for_tx_redeem(bitcoin_wallet)
                                .await
                                .context("Failed to check for existence of tx_redeem")
                                .map_err(backoff::Error::transient)
                        }
                    },
                    None,
                    None,
                )
                .await?
                {
                    return Ok::<_, anyhow::Error>(state5);
                }

                tokio::time::sleep(force_lookup_interval).await;
            }
        };

        tokio::select! {
            result = watch_for_redeem => result.expect("we never stop retrying to watch for Bitcoin redeem transaction"),
            result = force_lookup => result.expect("we never stop retrying to check for existence of tx_redeem"),
        }
    }
}

pub(super) trait RecvTransferProof {
    async fn infallible_recv_transfer_proof(
        &self,
        event_loop_handle: &mut SwapEventLoopHandle,
    ) -> monero::TransferProof;
}

impl RecvTransferProof for State3 {
    async fn infallible_recv_transfer_proof(
        &self,
        event_loop_handle: &mut SwapEventLoopHandle,
    ) -> monero::TransferProof {
        // TODO: Use a cleaner retry mechanism here
        // We cannot use the retry function here because we need mut access to the handle
        // Maybe we can use some macro here?
        loop {
            match event_loop_handle.recv_transfer_proof().await {
                Ok(proof) => return proof,
                Err(e) => {
                    tracing::warn!("Failed to receive transfer proof: {:#}, retrying in 1s", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
