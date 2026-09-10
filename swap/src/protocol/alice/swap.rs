//! Run an XMR/BTC swap in the role of Alice.
//! Alice holds XMR and wishes receive BTC.
use std::sync::Arc;
use std::time::Duration;

use crate::asb::{EventLoopHandle, LatestRate};
use crate::common::retry;
use crate::monero;
use crate::monero::TransferProof;
use crate::xkr::XkrWallet;
use crate::protocol::alice::{AliceState, Swap};
use ::bitcoin::consensus::encode::serialize_hex;
use anyhow::{Context, Result, bail};
use bitcoin_wallet::BitcoinWallet;
use monero_interface::PublishTransaction;
use rust_decimal::Decimal;
use swap_core::bitcoin::ExpiredTimelocks;
use swap_core::monero::BlockHeight;
use swap_env::env::Config;
use swap_machine::alice::State3;
use tokio::select;
use tokio::time::timeout;
use uuid::Uuid;

pub async fn run<LR>(swap: Swap, rate_service: LR) -> Result<AliceState>
where
    LR: LatestRate + Clone,
{
    run_until(swap, |_| false, rate_service).await
}

#[tracing::instrument(name = "swap", skip(swap,exit_early,rate_service), fields(id = %swap.swap_id), err)]
pub async fn run_until<LR>(
    mut swap: Swap,
    exit_early: fn(&AliceState) -> bool,
    rate_service: LR,
) -> Result<AliceState>
where
    LR: LatestRate + Clone,
{
    let mut current_state = swap.state;

    while !swap_machine::alice::is_complete(&current_state) && !exit_early(&current_state) {
        current_state = next_state(
            swap.swap_id,
            current_state,
            &mut swap.event_loop_handle,
            swap.bitcoin_wallet.clone(),
            &swap.env_config,
            rate_service.clone(),
        )
        .await?;

        retry(
            "Persisting latest Alice state",
            || {
                let db = swap.db.clone();
                let state = current_state.clone();

                async move {
                    db.insert_latest_state(swap.swap_id, state.into())
                        .await
                        .map_err(backoff::Error::transient)
                }
            },
            None,
            None,
        )
        .await
        .expect("we never stop retrying to persist the latest Alice state");
    }

    Ok(current_state)
}

async fn next_state<LR>(
    swap_id: Uuid,
    state: AliceState,
    event_loop_handle: &mut EventLoopHandle,
    bitcoin_wallet: Arc<dyn BitcoinWallet>,
    env_config: &Config,
    mut rate_service: LR,
) -> Result<AliceState>
where
    LR: LatestRate,
{
    let rate = rate_service
        .latest_rate()
        .map_or("NaN".to_string(), |rate| format!("{}", rate));

    tracing::info!(%state, %rate, "Advancing state");

    Ok(match state {
        AliceState::Started { state3 } => {
            let tx_lock_status = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_lock.clone()))
                .await;

            match timeout(
                env_config.bitcoin_lock_mempool_timeout,
                tx_lock_status.wait_until_seen(),
            )
            .await
            {
                Err(_) => {
                    tracing::info!(
                        minutes = %env_config.bitcoin_lock_mempool_timeout.as_secs_f64() / 60.0,
                        "TxLock lock was not seen in mempool in time. Alice might have denied our offer.",
                    );
                    AliceState::SafelyAborted
                }
                Ok(res) => {
                    res?;
                    AliceState::BtcLockTransactionSeen { state3 }
                }
            }
        }
        AliceState::BtcLockTransactionSeen { state3 } => {
            let tx_lock_status = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_lock.clone()))
                .await;

            match timeout(
                env_config.bitcoin_lock_confirmed_timeout,
                tx_lock_status.wait_until_final(),
            )
            .await
            {
                Err(_) => {
                    tracing::info!(
                        confirmations_needed = %env_config.bitcoin_finality_confirmations,
                        minutes = %env_config.bitcoin_lock_confirmed_timeout.as_secs_f64() / 60.0,
                        "TxLock lock did not get enough confirmations in time",
                    );

                    AliceState::BtcEarlyRefundable { state3 }
                }
                Ok(res) => {
                    res?;
                    AliceState::BtcLocked { state3 }
                }
            }
        }
        AliceState::BtcLocked { state3 } => {
            // Sometimes locking the Monero can fail e.g due to the daemon not being fully synced
            // We will retry indefinitely to lock the Monero funds, until either:
            // - the cancel timelock expires
            // - we do not manage to lock the Monero funds within the timeout
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(Some(env_config.monero_lock_retry_timeout))
                .with_max_interval(Duration::from_secs(30))
                .build();

            let constructed = backoff::future::retry_notify(
                backoff,
                || async {
                    // We check the status of the Bitcoin lock transaction
                    // If the swap is cancelled, there is no need to lock the Monero funds anymore
                    // because there is no way for the swap to succeed.
                    if !cancel_timelock_not_expired(&state3, &*bitcoin_wallet)
                        .await
                        .context("Failed to check for expired timelocks before locking Monero")
                        .map_err(backoff::Error::transient)?
                    {
                        return Ok::<_, backoff::Error<anyhow::Error>>(None);
                    }

                    // XKR restore height. TODO: query the XKR daemon height; 0 scans
                    // from genesis (correct, but slower for the refund wallet).
                    let monero_wallet_restore_blockheight = 0u64;

                    // The agreed lock amount and the shared 2-of-2 address to lock into.
                    // Hermes funding + developer tip are dropped in the XKR port
                    // (single-destination); the lock sends only the swap amount.
                    let req = state3.lock_xmr_transfer_request();
                    let amount = crate::xkr::to_xkr_atomic(req.amount);
                    let xkr = XkrWallet::from_env();

                    let shared_address = xkr
                        .shared_address(
                            req.public_spend_key.as_bytes(),
                            req.public_view_key.0.as_bytes(),
                        )
                        .await
                        .context("Failed to derive shared XKR address")
                        .map_err(backoff::Error::transient)?;

                    // Fund the lock from the ASB's own XKR wallet.
                    let (asb_spend, asb_view) = XkrWallet::asb_keys_from_env()
                        .context("ASB XKR keys not configured")
                        .map_err(backoff::Error::transient)?;

                    let txid = xkr
                        .lock_send(asb_spend, asb_view, &shared_address, amount, None)
                        .await
                        .context("Failed to send XKR lock transaction")
                        .map_err(backoff::Error::transient)?;

                    Ok::<_, backoff::Error<anyhow::Error>>(Some((
                        monero_wallet_restore_blockheight,
                        txid.clone(),
                        // tx_key is unused on the XKR side (Bob detects the lock by
                        // view-key scan); keep the TransferProof shape with a placeholder.
                        TransferProof::new(monero::TxHash(txid), placeholder_tx_key()),
                    )))
                },
                |e, wait_time: Duration| {
                    tracing::warn!(
                        swap_id = %swap_id,
                        error = ?e,
                        "Failed to construct Monero lock transaction. We will retry in {} seconds",
                        wait_time.as_secs()
                    )
                },
            )
            .await;

            match constructed {
                // If the construction was successful, we transition to the next state
                Ok(Some((monero_wallet_restore_blockheight, xmr_lock_txid, transfer_proof))) => {
                    AliceState::XmrLockTransactionConstructed {
                        monero_wallet_restore_blockheight: BlockHeight {
                            height: monero_wallet_restore_blockheight,
                        },
                        xmr_lock_txid,
                        transfer_proof,
                        state3,
                    }
                }
                // If we were not able to lock the Monero funds before the timelock expired,
                // we can safely abort the swap because we did not lock any funds
                // We do not do an early refund because Bob can refund himself (timelock expired)
                Ok(None) => {
                    tracing::info!(
                        swap_id = %swap_id,
                        "We did not manage to lock the Monero funds before the timelock expired. Aborting swap."
                    );

                    AliceState::SafelyAborted
                }
                Err(e) => {
                    tracing::error!(
                        swap_id = %swap_id,
                        error = ?e,
                        "Failed to lock Monero within {} seconds. We will do an early refund of the Bitcoin. We didn't lock any Monero funds so this is safe.",
                        env_config.monero_lock_retry_timeout.as_secs()
                    );

                    AliceState::BtcEarlyRefundable { state3 }
                }
            }
        }
        AliceState::BtcEarlyRefundable { state3 } => {
            if let Some(tx_early_refund) = state3.signed_early_refund_transaction() {
                let tx_early_refund = tx_early_refund?;
                let tx_early_refund_txid = tx_early_refund.compute_txid();

                // Bob might cancel the swap and refund for himself. We won't need to early refund anymore.
                let tx_cancel_status = bitcoin_wallet
                    .subscribe_to(Box::new(state3.tx_cancel()))
                    .await;

                let backoff = backoff::ExponentialBackoffBuilder::new()
                    // We give up after 6 hours
                    // (Most likely Bob the a Replace-by-Fee on the tx_lock transaction)
                    .with_max_elapsed_time(Some(Duration::from_secs(6 * 60 * 60)))
                    // We wait a while between retries
                    .with_max_interval(Duration::from_secs(10 * 60))
                    .build();

                // Concurrently retry to broadcast the early refund transaction
                // and wait for the cancel transaction to be broadcasted.
                tokio::select! {
                    // If Bob cancels the swap, he can refund himself.
                    // Nothing for us to do anymore.
                    result = tx_cancel_status.wait_until_seen() => {
                        result?;
                        AliceState::SafelyAborted
                    }

                    // Retry repeatedly to broadcast tx_early_refund
                    result = async {
                        backoff::future::retry_notify(backoff, || async {
                            bitcoin_wallet.ensure_broadcasted(tx_early_refund.clone(), "early_refund").await.map_err(backoff::Error::transient)
                        }, |e, wait_time: Duration| {
                            tracing::warn!(
                                %tx_early_refund_txid,
                                error = ?e,
                                "Failed to broadcast early refund transaction. We will retry in {} seconds",
                                wait_time.as_secs()
                            )
                        })
                        .await
                    } => {
                        match result {
                            Ok((_txid, _subscription)) => {
                                tracing::info!(
                                    %tx_early_refund_txid,
                                    "Refunded Bitcoin early for Bob"
                                );

                                AliceState::BtcEarlyRefunded(state3)
                            }
                            Err(e) => {
                                tracing::error!(
                                    %tx_early_refund_txid,
                                    error = ?e,
                                    "Failed to broadcast early refund transaction after retries exhausted. Bob will have to wait for the timelock to expire then refund himself."
                                );
                                AliceState::SafelyAborted
                            }
                        }
                    }
                }
            } else {
                // We do not have Bob's signature for the early refund transaction
                // Therefore we cannot do an early refund.
                // We abort the swap on our side.
                // Bob will have to wait for the timelock to expire then refund himself.
                AliceState::SafelyAborted
            }
        }
        AliceState::XmrLockTransactionConstructed {
            monero_wallet_restore_blockheight,
            xmr_lock_txid,
            transfer_proof,
            state3,
        } => {
            // The XKR lock was already broadcast atomically in the construct step
            // (gated there by the cancel-timelock check). Unlike Monero's separate
            // publish step, there is nothing to broadcast here.
            //
            // NOTE: because the send is atomic, the construct-time timelock check is
            // the only guard; a crash between broadcast and state-persist can leave
            // XKR locked after the cancel timelock. This is inherent to the XKR
            // wallet's build+broadcast-in-one model.
            tracing::info!(%swap_id, txid = %xmr_lock_txid, "XKR lock transaction is broadcast");

            AliceState::XmrLockTransactionSent {
                monero_wallet_restore_blockheight,
                transfer_proof,
                state3,
            }
        }
        AliceState::XmrLockTransactionSent {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => match state3.expired_timelocks(&*bitcoin_wallet).await? {
            ExpiredTimelocks::None { .. } => {
                tracing::info!("Locked XKR, waiting for confirmations");

                // Confirm via the ASB's own wallet, which sees the lock as an
                // outgoing transaction. Best-effort: the lock is already broadcast,
                // and Bob independently detects it by view-key scanning.
                let xkr = XkrWallet::from_env();
                match XkrWallet::asb_keys_from_env() {
                    Ok((asb_spend, asb_view)) => {
                        if let Err(e) = xkr
                            .wait_until_confirmed(asb_spend, asb_view, &transfer_proof.tx_hash().0, 1)
                            .await
                        {
                            tracing::warn!(%swap_id, err = %e, "Failed to confirm XKR lock; proceeding");
                        }
                    }
                    Err(e) => tracing::warn!(%swap_id, err = %e, "ASB XKR keys not configured; skipping lock confirmation"),
                }

                AliceState::XmrLocked {
                    monero_wallet_restore_blockheight,
                    transfer_proof,
                    state3,
                }
            }
            _ => AliceState::CancelTimelockExpired {
                monero_wallet_restore_blockheight,
                transfer_proof,
                state3,
            },
        },
        AliceState::XmrLocked {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            let tx_lock_status = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_lock.clone()))
                .await;

            tokio::select! {
                result = event_loop_handle.send_transfer_proof(transfer_proof.clone()) => {
                   result?;

                   AliceState::XmrLockTransferProofSent {
                       monero_wallet_restore_blockheight,
                       transfer_proof,
                       state3,
                   }
                },
                // If we send Bob the transfer proof, but for whatever reason we do not receive an acknowledgement from him
                // we would be stuck in this state forever until the timelock expires.
                //
                // By listening for the encrypted signature here we can still proceed to the next state
                // even if Bob does not respond with an acknowledgement but sends us the encrypted signature immediately.
                enc_sig = event_loop_handle.recv_encrypted_signature() => {
                    tracing::info!("Received encrypted signature via p2p channel. We haven't verified it yet.");

                    AliceState::EncSigLearned {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        encrypted_signature: Box::new(enc_sig?),
                        state3,
                    }
                }
                result = tx_lock_status.wait_until_confirmed_with(state3.cancel_timelock) => {
                    result?;
                    AliceState::CancelTimelockExpired {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    }
                }
            }
        }
        AliceState::XmrLockTransferProofSent {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            let tx_lock_status_subscription = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_lock.clone()))
                .await;

            select! {
                biased;
                result = tx_lock_status_subscription.wait_until_confirmed_with(state3.cancel_timelock) => {
                    result?;
                    AliceState::CancelTimelockExpired {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    }
                }
                enc_sig = event_loop_handle.recv_encrypted_signature() => {
                    tracing::info!("Received encrypted signature");

                    AliceState::EncSigLearned {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        encrypted_signature: Box::new(enc_sig?),
                        state3,
                    }
                }
                burn_instruction = event_loop_handle.wait_for_burn_on_refund_instruction() => {
                    let burn = burn_instruction.context("Failed to receive burn instruction")?;
                    let mut updated_state3 = (*state3).clone();
                    updated_state3.should_publish_tx_withhold = Some(burn);

                    AliceState::XmrLockTransferProofSent {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3: Box::new(updated_state3),
                    }
                }
            }
        }
        AliceState::EncSigLearned {
            monero_wallet_restore_blockheight,
            transfer_proof,
            encrypted_signature,
            state3,
        } => {
            // Try to sign the Bitcoin redeem transactions
            let tx_redeem = match state3.signed_redeem_transaction(*encrypted_signature) {
                Ok(tx_redeem) => tx_redeem,
                // If we cannot sign the transaction there must be something wrong
                // We just wait for the cancel timelock to expire and then refund
                Err(error) => {
                    tracing::error!(
                        "Failed to construct redeem transaction: {:#}, we will wait for the cancel timelock expiration to refund",
                        error
                    );

                    return Ok(AliceState::WaitingForCancelTimelockExpiration {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    });
                }
            };

            // Retry indefinitely to publish the redeem transaction, until the cancel timelock expires
            // Publishing the redeem transaction might fail on the first try due to any number of reasons
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(None)
                .with_max_interval(Duration::from_secs(60))
                .build();

            match backoff::future::retry_notify(backoff.clone(), || async {
                let tx_lock_status = bitcoin_wallet
                    .status_of_script(&state3.tx_lock.clone())
                    .await?;

                // If the cancel timelock is expired, it it not safe to publish the Bitcoin redeem transaction anymore
                //
                // TODO: In practice this should be redundant because the logic above will trigger for a superset of the cases where this is true
                if tx_lock_status.is_confirmed_with(state3.cancel_timelock) {
                    return Ok(None);
                }

                // We can only redeem the Bitcoin if we are fairly sure that our Bitcoin redeem transaction
                // will be confirmed before the cancel timelock expires
                //
                // We make an assumption that it will take at most `env_config.bitcoin_blocks_till_confirmed_upper_bound_assumption` blocks
                // until our transaction is included in a block. If this assumption is not satisfied, we will not publish the transaction.
                //
                // We will instead wait for the cancel timelock to expire and then refund.
                if tx_lock_status.blocks_left_until(state3.cancel_timelock) < env_config.bitcoin_blocks_till_confirmed_upper_bound_assumption {
                    return Ok(None);
                }

                bitcoin_wallet
                    .ensure_broadcasted(tx_redeem.clone(), "redeem")
                    .await
                    .map(Some)
                    .map_err(backoff::Error::transient)
            }, |e, wait_time: Duration| {
                tracing::warn!(
                    swap_id = %swap_id,
                    error = ?e,
                    "Failed to broadcast Bitcoin redeem transaction. We will retry in {} seconds",
                    wait_time.as_secs()
                )
            })
            .await
            .expect("We should never run out of retries while publishing the Bitcoin redeem transaction")
            {
                // We successfully published the redeem transaction
                // We wait until we see the transaction in the mempool before transitioning to the next state
                Some((txid, subscription)) => match subscription.wait_until_seen().await {
                    Ok(_) => AliceState::BtcRedeemTransactionPublished { state3, transfer_proof },
                    // TODO: No need to bail here, we should just retry?
                    Err(e) => {
                        // We extract the txid and the hex representation of the transaction
                        // this'll allow the user to manually re-publish the transaction
                        let tx_hex = serialize_hex(&tx_redeem);

                        bail!("Waiting for Bitcoin redeem transaction to be in mempool failed with {}! The redeem transaction was published, but it is not ensured that the transaction was included! You might be screwed. You can try to manually re-publish the transaction (TxID: {}, Tx Hex: {})", e, txid, tx_hex)
                    }
                },

                // It is not safe to publish the Bitcoin redeem transaction anymore
                // We wait for the cancel timelock to expire and then refund
                None => {
                    tracing::error!("We were unable to publish the Bitcoin redeem transaction before the timelock expired.");

                    AliceState::WaitingForCancelTimelockExpiration {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    }
                }
            }
        }
        AliceState::BtcRedeemTransactionPublished { state3, .. } => {
            let subscription = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_redeem()))
                .await;

            match subscription.wait_until_final().await {
                Ok(_) => AliceState::BtcRedeemed,
                Err(e) => {
                    bail!(
                        "The Bitcoin redeem transaction was seen in mempool, but waiting for finality timed out with {}. Manual investigation might be needed to ensure that the transaction was included.",
                        e
                    )
                }
            }
        }
        AliceState::WaitingForCancelTimelockExpiration {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            let tx_lock_status_subscription = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_lock.clone()))
                .await;

            select! {
                result = tx_lock_status_subscription.wait_until_confirmed_with(state3.cancel_timelock) => {
                    result?;
                    AliceState::CancelTimelockExpired {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    }
                }
                burn_instruction = event_loop_handle.wait_for_burn_on_refund_instruction() => {
                    let burn = burn_instruction.context("Failed to receive burn instruction")?;
                    let mut updated_state3 = (*state3).clone();
                    updated_state3.should_publish_tx_withhold = Some(burn);

                    AliceState::WaitingForCancelTimelockExpiration {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3: Box::new(updated_state3),
                    }
                }
            }
        }
        AliceState::CancelTimelockExpired {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(None)
                // No need to be super aggressive here
                .with_max_interval(Duration::from_secs(60 * 10))
                .build();

            backoff::future::retry_notify::<_, anyhow::Error, _, _, _, _>(
                backoff,
                || async {
                    if state3
                        .check_for_tx_cancel(&*bitcoin_wallet)
                        .await
                        .context("Failed to check for existence of Bitcoin cancel transaction on chain")
                        .map_err(backoff::Error::transient)?
                        .is_some()
                    {
                        return Ok(());
                    }

                    state3
                        .submit_tx_cancel(&*bitcoin_wallet)
                        .await
                        .context("Failed to submit cancel transaction")
                        .map_err(backoff::Error::transient)?;

                    Ok(())
                },
                |e: anyhow::Error, wait_time: Duration| {
                    tracing::warn!(
                        swap_id = %swap_id,
                        error = ?e,
                        "Failed to ensure cancel transaction is published. We will retry in {} seconds",
                        wait_time.as_secs()
                    )
                },
            )
            .await
            .expect("We should never run out of retries while ensuring the cancel transaction is published");

            AliceState::BtcCancelled {
                monero_wallet_restore_blockheight,
                transfer_proof,
                state3,
            }
        }
        AliceState::BtcCancelled {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            let tx_cancel_status = bitcoin_wallet
                .subscribe_to(Box::new(state3.tx_cancel()))
                .await;

            // We wait for either TxFullRefund or TxPartialRefund to be published
            // - both allow us to extract the Monero refund key.
            // Otherwise we punish, once that timelock expired.

            select! {
                spend_key = state3.watch_for_btc_tx_full_refund(&*bitcoin_wallet) => {
                    let spend_key = spend_key?;

                    AliceState::BtcRefunded {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        spend_key,
                        state3,
                    }
                }
                spend_key = state3.watch_for_btc_tx_partial_refund(&*bitcoin_wallet), if state3.btc_amnesty_amount.is_some() => {
                    let spend_key = spend_key?;

                    AliceState::BtcRefunded {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        spend_key,
                        state3,
                    }
                }
                result = tx_cancel_status.wait_until_confirmed_with(state3.punish_timelock) => {
                    result?;

                    AliceState::BtcPunishable {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3,
                    }
                }
                burn_instruction = event_loop_handle.wait_for_burn_on_refund_instruction() => {
                    let burn = burn_instruction.context("Failed to receive burn instruction")?;
                    let mut updated_state3 = (*state3).clone();
                    updated_state3.should_publish_tx_withhold = Some(burn);

                    tracing::info!(withhold=%burn, "Received withhold decision");

                    AliceState::BtcCancelled {
                        monero_wallet_restore_blockheight,
                        transfer_proof,
                        state3: Box::new(updated_state3),
                    }
                }
            }
        }
        AliceState::BtcRefunded {
            transfer_proof,
            spend_key,
            state3,
            monero_wallet_restore_blockheight,
        } => AliceState::XmrRefundable {
            monero_wallet_restore_blockheight,
            transfer_proof,
            spend_key,
            state3,
        },
        AliceState::BtcPartiallyRefunded {
            transfer_proof,
            spend_key,
            state3,
            monero_wallet_restore_blockheight,
        } => AliceState::XmrRefundable {
            monero_wallet_restore_blockheight,
            transfer_proof,
            spend_key,
            state3,
        },
        AliceState::XmrRefundable {
            monero_wallet_restore_blockheight: _,
            transfer_proof: _,
            spend_key,
            state3,
        } => {
            // `spend_key` is the combined shared spend key (s_a + s_b, extracted
            // from Bob's BTC refund). Combined with the shared view secret, Alice
            // reconstructs the shared XKR wallet and sweeps the locked output back
            // to the ASB's refund address. This reuses the sweep (redeem) path.
            let shared_spend = spend_key.as_bytes();
            let shared_view = state3.xmr_shared_view_secret();
            let refund_address = std::env::var("XKR_ASB_REFUND_ADDRESS")
                .context("XKR_ASB_REFUND_ADDRESS not set")?;
            let xkr = XkrWallet::from_env();

            let xmr_refund_txid = retry(
                "Refund XKR",
                || {
                    let xkr = xkr.clone();
                    let refund_address = refund_address.clone();
                    async move {
                        xkr.redeem(shared_spend, shared_view, &refund_address, None)
                            .await
                            .map_err(backoff::Error::transient)
                    }
                },
                None,
                Duration::from_secs(60),
            )
            .await
            .expect("We should never run out of retries while refunding XKR");

            AliceState::XmrRefundTxConstructed {
                state3,
                xmr_refund_txid,
            }
        }
        AliceState::XmrRefundTxConstructed {
            state3,
            xmr_refund_txid,
        } => {
            // The XKR refund sweep already broadcast atomically in the previous step.
            tracing::info!(%swap_id, txid = %xmr_refund_txid, "XKR refund sweep is broadcast");

            AliceState::XmrRefundTxPublished {
                state3,
                xmr_refund_txid,
            }
        }
        AliceState::XmrRefundTxPublished {
            state3,
            xmr_refund_txid,
        } => {
            // The refund sweep is broadcast; Alice has reclaimed her funds. On-chain
            // confirmation is skipped here because this state does not carry the
            // shared keys needed to re-import the wallet for a confirm poll.
            tracing::info!(%swap_id, txid = %xmr_refund_txid, "XKR refund sweep broadcast; funds reclaimed");

            AliceState::XmrRefunded {
                state3: Some(state3),
            }
        }
        AliceState::BtcPunishable {
            monero_wallet_restore_blockheight,
            transfer_proof,
            state3,
        } => {
            retry(
                "Punish Bitcoin",
                || async {
                    // Before punishing, we explicitly check for the refund transaction as we prefer refunds over punishments
                    let spend_key_from_btc_refund = state3.refund_btc(&*bitcoin_wallet).await.context("Failed to check for existence of Bitcoin refund transaction before punishing").map_err(backoff::Error::transient)?;

                    // If we find the Bitcoin refund transaction, we go ahead and refund the Monero
                    if let Some(spend_key_from_btc_refund) = spend_key_from_btc_refund {
                        return Ok::<AliceState, backoff::Error<anyhow::Error>>(AliceState::BtcRefunded {
                            monero_wallet_restore_blockheight,
                            transfer_proof: transfer_proof.clone(),
                            spend_key: spend_key_from_btc_refund,
                            state3: state3.clone(),
                        });
                    }

                    state3.punish_btc(&*bitcoin_wallet).await.context("Failed to construct and publish Bitcoin punish transaction").map_err(backoff::Error::transient)?;

                    Ok::<AliceState, backoff::Error<anyhow::Error>>(AliceState::BtcPunished {
                        state3: state3.clone(),
                        transfer_proof: transfer_proof.clone(),
                    })
                },
                None,
                // We can take our time when punishing
                Duration::from_secs(60 * 5),
            )
            .await
            .expect("We should never run out of retries while publishing the punish transaction")
        }
        AliceState::XmrRefunded { state3 } => {
            // Only publish TxWithhold for swaps which have an anti-spam deposit.
            let Some(mut state3) = state3 else {
                tracing::info!(
                    "Running a pre-partial refund swap, there is no anti-spam deposit to withhold"
                );
                return Ok(AliceState::XmrRefunded { state3: None });
            };

            // Fetch the burn decision again, incase it was updated via the controller
            if let Some(burn_decision) = event_loop_handle.get_burn_on_refund_instruction().await {
                state3.should_publish_tx_withhold = Some(burn_decision);
            }

            // Skip publishing TxWithhold unless we were specifically instructed
            if !state3.should_publish_tx_withhold.unwrap_or(false) {
                tracing::info!("Not instructed to withhold the anti-spam deposit. Finishing");
                return Ok(AliceState::XmrRefunded {
                    state3: Some(state3),
                });
            }

            retry("Publish TxWithhold", || {
                let state3 = state3.clone();
                let bitcoin_wallet = bitcoin_wallet.clone();

                async move {
                    let signed_tx = state3.signed_withhold_transaction()
                        .context("Can't withhold the anti-spam deposit after Bob refunded because we couldn't construct the transaction")
                        .map_err(backoff::Error::transient)?;

                    bitcoin_wallet
                        .ensure_broadcasted(signed_tx, "withhold")
                        .await
                        .context("Couldn't publish TxWithhold")
                        .map_err(backoff::Error::transient)?;

                    Ok(AliceState::BtcWithholdPublished { state3 })
                }
            }, None, None).await?
        }
        AliceState::BtcWithholdPublished { state3 } => {
            retry(
                "Wait for TxWithhold confirmation",
                || {
                    let state3 = state3.clone();
                    let bitcoin_wallet = bitcoin_wallet.clone();

                    async move {
                        let tx_withhold = state3
                            .tx_withhold()
                            .context("Can't construct TxWithhold even though we published it")
                            .map_err(backoff::Error::transient)?;

                        let subscription = bitcoin_wallet.subscribe_to(Box::new(tx_withhold)).await;

                        subscription
                            .wait_until_final()
                            .await
                            .context("Failed to wait for TxWithhold to be confirmed")
                            .map_err(backoff::Error::transient)?;

                        Ok(AliceState::BtcWithholdConfirmed { state3 })
                    }
                },
                None,
                None,
            )
            .await?
        }
        AliceState::BtcWithholdConfirmed { state3 } => {
            // Nothing to do here. Mercy is triggered manually.
            AliceState::BtcWithholdConfirmed { state3 }
        }
        AliceState::BtcMercyGranted { state3 } => {
            retry(
                "Publish TxMercy",
                || {
                    let state3 = state3.clone();
                    let bitcoin_wallet = bitcoin_wallet.clone();

                    async move {
                        let signed_tx = state3
                            .signed_mercy_transaction()
                            .context("Failed to construct signed TxMercy")
                            .map_err(backoff::Error::transient)?;

                        bitcoin_wallet
                            .ensure_broadcasted(signed_tx, "mercy")
                            .await
                            .context("Failed to publish TxMercy")
                            .map_err(backoff::Error::transient)?;

                        tracing::info!("TxMercy published successfully");

                        Ok(AliceState::BtcMercyPublished { state3 })
                    }
                },
                None,
                None,
            )
            .await?
        }
        AliceState::BtcMercyPublished { state3 } => {
            retry(
                "Wait for TxMercy confirmation",
                || {
                    let state3 = state3.clone();
                    let bitcoin_wallet = bitcoin_wallet.clone();

                    async move {
                        let tx_mercy = state3
                            .tx_mercy()
                            .context("Couldn't construct TxMercy even though we have published it")
                            .map_err(backoff::Error::transient)?;

                        let subscription = bitcoin_wallet.subscribe_to(Box::new(tx_mercy)).await;

                        subscription
                            .wait_until_final()
                            .await
                            .context("Failed to wait for TxMercy to be confirmed")
                            .map_err(backoff::Error::transient)?;

                        Ok(AliceState::BtcMercyConfirmed { state3 })
                    }
                },
                None,
                None,
            )
            .await?
        }
        AliceState::BtcMercyConfirmed { state3 } => AliceState::BtcMercyConfirmed { state3 },
        AliceState::BtcRedeemed => AliceState::BtcRedeemed,
        AliceState::BtcPunished {
            state3,
            transfer_proof,
        } => AliceState::BtcPunished {
            state3,
            transfer_proof,
        },
        AliceState::BtcEarlyRefunded(state3) => AliceState::BtcEarlyRefunded(state3),
        AliceState::SafelyAborted => AliceState::SafelyAborted,
    })
}

/// A placeholder Monero tx key for the XKR `TransferProof`. XKR locks are detected
/// by Bob via view-key scanning, so the `tx_key` field is unused; we keep the
/// `TransferProof` shape (and the p2p message) unchanged and fill a fixed valid key.
fn placeholder_tx_key() -> monero_oxide_ext::PrivateKey {
    let mut bytes = [0u8; 32];
    bytes[0] = 1;
    monero_oxide_ext::PrivateKey::from_slice(&bytes).expect("scalar 1 is a valid private key")
}

/// This function is used to check if Alice is in a state where it is clear that she has already received the encrypted signature from Bob.
/// This allows us to acknowledge the encrypted signature multiple times
/// If our acknowledgement does not reach Bob, he might send the encrypted signature again.
pub(crate) fn has_already_processed_enc_sig(state: &AliceState) -> bool {
    matches!(
        state,
        AliceState::EncSigLearned { .. }
            | AliceState::BtcRedeemTransactionPublished { .. }
            | AliceState::BtcRedeemed
    )
}

async fn cancel_timelock_not_expired(
    state3: &State3,
    bitcoin_wallet: &dyn BitcoinWallet,
) -> Result<bool> {
    Ok(matches!(
        state3.expired_timelocks(bitcoin_wallet).await?,
        ExpiredTimelocks::None { .. }
    ))
}

