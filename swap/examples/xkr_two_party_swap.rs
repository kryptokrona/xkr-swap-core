//! Level-3 end-to-end driver: a full two-party BTC<->XKR atomic swap, with both
//! parties (Alice/maker and Bob/taker) running in-process against real infra.
//!
//! This is the one thing the Level-1 (JS service) and Level-2 (adapter) tests do
//! not prove: the whole cross-chain choreography end to end —
//!
//!   Bob locks BTC  ->  Alice sees it confirmed, locks XKR  ->  Bob detects the
//!   XKR lock and sends his encrypted signature  ->  Alice redeems the BTC,
//!   revealing the adaptor  ->  Bob extracts the key and sweeps the XKR.
//!
//! The two run on connected in-process libp2p swarms (Alice listens, Bob dials),
//! exactly as `swap-asb` and the `swap` CLI wire them in production. Everything
//! external is orchestrated by the harness that invokes this
//! (`scripts/testnet/swap-two-party-test.sh`):
//!   * a bitcoind regtest + electrs for the BTC side (this driver funds Bob and
//!     the harness mines BTC blocks in the background to drive confirmations),
//!   * a peered XKR testnet mesh + the `xkr-wallet-rpc.cjs` service for the XKR
//!     side (the harness funds the ASB's XKR wallet and mines XKR blocks).
//!
//! The engine reaches the XKR side entirely through env (`XKR_WALLET_RPC_URL`,
//! `XKR_ASB_SPEND_SECRET`, `XKR_ASB_VIEW_SECRET`) inside the protocol code, so
//! this driver never touches `XkrWallet` directly — it only stands up the two
//! parties and asserts they both reach their happy-path terminal states.
//!
//! Config (all via env):
//!   ELECTRUM_RPC_URL     electrs, e.g. tcp://@localhost:50001
//!   BITCOIND_RPC_URL     bitcoind wallet RPC, e.g.
//!                        http://user:pass@127.0.0.1:18443/wallet/xkr
//!   XKR_WALLET_RPC_URL   XKR wallet RPC service (read by the engine)
//!   XKR_ASB_SPEND_SECRET / XKR_ASB_VIEW_SECRET   the ASB's XKR keys (engine)
//!   XKR_RECEIVE_ADDRESS  Bob's XKR payout address (the redeem sweep destination)
//!   BTC_AMOUNT_SAT       swap size in satoshis (default 1_000_000)
//!   SWAP_TIMEOUT_SECS    overall wall-clock budget (default 900)

use anyhow::{Context, Result, bail};
use bitcoin::Amount;
use bitcoin_wallet::{PersisterConfig, Wallet, WalletBuilder};
use libp2p::Multiaddr;
use rust_decimal::Decimal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use swap::asb::{self, FixedRate};
use swap::cli;
use swap::database::{AccessMode, SqliteDatabase};
use swap::monero::MoneroAddressPool;
use swap::network::rendezvous::XmrBtcNamespace;
use swap::network::swarm;
use swap::protocol::Database as _;
use swap::protocol::{alice, bob};
use swap::seed::Seed;
use swap_env::config::{RefundPolicy, default_btc_redeem_fee_multiplier};
use swap_env::env::{Config, GetConfig, Regtest};
use tokio::time::timeout;
use uuid::Uuid;

/// A valid mainnet Monero address literal, used only to satisfy `bob::Swap::new`'s
/// vestigial `monero_receive_pool` argument. The XKR port routes Bob's payout to
/// `xkr_receive_address`, so this pool is never used on the happy path — it just
/// has to construct.
const DUMMY_XMR_ADDRESS: &str = "4B33mFPMq6mKi7Eiyd5XuyKRVMGVZz1Rqb9ZTyGApXW5d1aT7UBDZ89ewmnWFkzJ5wPd2SFbn313vCT8a4E2Qf4KQH4pNey";

fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing env var {key}"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Minimal bitcoind JSON-RPC call. `BITCOIND_RPC_URL` may embed `user:pass@`
/// userinfo (we lift it into HTTP basic auth) and a `/wallet/<name>` path.
async fn btc_rpc(rpc_url: &str, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
    let parsed = url::Url::parse(rpc_url).context("parse BITCOIND_RPC_URL")?;
    let user = parsed.username().to_string();
    let pass = parsed.password().unwrap_or("").to_string();
    let mut clean = parsed.clone();
    let _ = clean.set_username("");
    let _ = clean.set_password(None);

    let body = serde_json::json!({
        "jsonrpc": "1.0",
        "id": "xkr-l3",
        "method": method,
        "params": params,
    });

    let resp: serde_json::Value = reqwest::Client::new()
        .post(clean.as_str())
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("bitcoind {method} request"))?
        .json()
        .await
        .with_context(|| format!("bitcoind {method} decode"))?;

    if !resp["error"].is_null() {
        bail!("bitcoind {method} error: {}", resp["error"]);
    }
    Ok(resp["result"].clone())
}

/// Build a BDK bitcoin wallet backed by the regtest electrs, mirroring how the
/// production CLI/ASB build theirs (in-memory sqlite, 1-conf finality).
async fn build_btc_wallet(
    seed: &Seed,
    electrum_url: &str,
    network: bitcoin::Network,
) -> Result<Arc<Wallet>> {
    let wallet = WalletBuilder::<Seed>::default()
        .seed(seed.clone())
        .network(network)
        .electrum_rpc_urls(vec![electrum_url.to_string()])
        .persister(PersisterConfig::InMemorySqlite)
        .finality_confirmations(1_u32)
        .target_block(1_u32)
        .sync_interval(Duration::from_secs(2))
        .use_mempool_space_fee_estimation(false)
        .build()
        .await
        .context("build btc wallet")?;
    Ok(Arc::new(wallet))
}

async fn open_db(path: &Path) -> Result<Arc<SqliteDatabase>> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    if !path.exists() {
        tokio::fs::File::create(path).await.context("create db file")?;
    }
    Ok(Arc::new(
        SqliteDatabase::open(path, AccessMode::ReadWrite)
            .await
            .context("open sqlite db")?,
    ))
}

/// Poll-sync a wallet until it sees at least `want`.
async fn wait_for_btc_balance(wallet: &Wallet, want: Amount) -> Result<()> {
    for attempt in 1..=60u32 {
        wallet.sync().await.context("sync btc wallet")?;
        let have = wallet.balance().await.context("btc balance")?;
        if have >= want {
            println!("[l3]   bob btc balance = {have}");
            return Ok(());
        }
        if attempt % 5 == 0 {
            println!("[l3]   waiting for bob btc funding ({have} / {want})");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    bail!("bob btc wallet never reached funding balance {want}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(env_or("RUST_LOG", "info,swap=debug,swap_p2p=debug"))
        .with_target(false)
        .init();

    let env_config: Config = Regtest::get_config();
    let btc_network = env_config.bitcoin_network;

    // electrum-client only strips the scheme, so no userinfo "@" in the URL:
    // `tcp://host:port` (a leading "@" ends up in the host and breaks DNS).
    let electrum_url = env_or("ELECTRUM_RPC_URL", "tcp://127.0.0.1:50001");
    let bitcoind_rpc = env("BITCOIND_RPC_URL")?;
    let btc_amount = Amount::from_sat(env_or("BTC_AMOUNT_SAT", "1000000").parse().context("BTC_AMOUNT_SAT")?);
    let bob_xkr_receive = env("XKR_RECEIVE_ADDRESS")?;
    let timeout_secs: u64 = env_or("SWAP_TIMEOUT_SECS", "900").parse().context("SWAP_TIMEOUT_SECS")?;

    // SWAP_MODE=refund exercises the safety path: swap setup + Bob's BTC lock
    // complete, but Alice never locks XKR, so once the cancel timelock expires Bob
    // must unilaterally reclaim his BTC (ending in BtcRefunded). Default "happy"
    // runs the full redeem-both-sides path.
    let refund_mode = env_or("SWAP_MODE", "happy") == "refund";
    if refund_mode {
        println!("[l3] mode: REFUND (Alice will not lock XKR; Bob must reclaim BTC via the cancel timelock)");
    }

    let scratch = std::env::temp_dir().join(format!("xkr-l3-{}", Uuid::new_v4()));

    println!("[l3] building bitcoin wallets (electrs {electrum_url}, network {btc_network:?})");
    let alice_seed = Seed::random().context("alice seed")?;
    let bob_seed = Seed::random().context("bob seed")?;
    let alice_btc = build_btc_wallet(&alice_seed, &electrum_url, btc_network).await?;
    let bob_btc = build_btc_wallet(&bob_seed, &electrum_url, btc_network).await?;

    // ---- Fund Bob with BTC from the regtest bitcoind, then confirm it. ----
    let bob_deposit = bob_btc.new_address().await.context("bob deposit address")?;
    let fund_sat = btc_amount.to_sat().saturating_mul(3);
    let fund_btc = Amount::from_sat(fund_sat).to_btc();
    println!("[l3] funding bob: sendtoaddress {bob_deposit} {fund_btc} BTC");
    btc_rpc(&bitcoind_rpc, "sendtoaddress", serde_json::json!([bob_deposit.to_string(), fund_btc])).await?;
    let mine_to = btc_rpc(&bitcoind_rpc, "getnewaddress", serde_json::json!([])).await?;
    let mine_to = mine_to.as_str().context("getnewaddress result")?;
    btc_rpc(&bitcoind_rpc, "generatetoaddress", serde_json::json!([3, mine_to])).await?;
    wait_for_btc_balance(&bob_btc, btc_amount).await?;

    // ---- Alice (maker): asb swarm + event loop, listening. ----
    let min_buy = Amount::from_sat(0);
    let max_buy = Amount::from_sat(u64::MAX);

    let alice_db_path = scratch.join("alice.sqlite");
    let alice_db = open_db(&alice_db_path).await?;

    let (mut alice_swarm, _onion_addrs, _onion) = swarm::asb(
        &alice_seed,
        min_buy,
        max_buy,
        FixedRate::default(),
        false, // resume_only
        env_config,
        XmrBtcNamespace::Testnet,
        &[], // rendezvous_addrs
        None, // tor client
        false, // register_hidden_service
        1_u8, // num_intro_points
        16_usize, // max_concurrent_rend_requests
        false, // wormhole_enabled
        3_usize, // wormhole_max_concurrent_rend_requests
        3_u8, // wormhole_num_intro_points
        168_u64, // wormhole_swap_freshness_hours
        alice_db.clone(), // trust_provider (SqliteDatabase impls PeerTrust)
        None, // metrics registry
    )
    .context("build alice swarm")?;

    // Pick a free port and listen on it, so Bob has a concrete address to dial.
    let alice_port = {
        let l = std::net::TcpListener::bind(("127.0.0.1", 0)).context("bind free port")?;
        l.local_addr().context("local_addr")?.port()
    };
    let alice_addr: Multiaddr = format!("/ip4/127.0.0.1/tcp/{alice_port}")
        .parse()
        .context("alice multiaddr")?;
    alice_swarm.listen_on(alice_addr.clone()).context("alice listen_on")?;

    let (alice_event_loop, mut alice_swap_rx, _alice_service) = asb::EventLoop::new(
        alice_swarm,
        None, // metrics
        env_config,
        alice_btc.clone(),
        alice_db.clone(),
        FixedRate::default(),
        min_buy,
        max_buy,
        None, // external_redeem_address
        default_btc_redeem_fee_multiplier(),
        Decimal::ZERO, // developer_tip
        RefundPolicy::default(),
        None, // onion_service_handle
        alice_db_path.with_extension("config.toml"),
    )
    .context("build alice event loop")?;

    let alice_peer_id = alice_event_loop.peer_id();
    println!("[l3] alice listening on {alice_addr}/p2p/{alice_peer_id}");
    tokio::spawn(alice_event_loop.run());

    // Alice creates a Swap when Bob initiates setup (via the event loop above).
    // Happy path: run it to completion (locks XKR, redeems BTC). Refund path:
    // receive it but never run it, so Alice completes setup — letting Bob lock
    // BTC — yet never locks XKR, forcing Bob down the cancel-timelock refund path.
    let alice_join = tokio::spawn(async move {
        let swap = alice_swap_rx
            .recv()
            .await
            .context("alice never received a swap from bob")?;
        if refund_mode {
            tracing::info!("refund mode: Alice received the swap but will NOT lock XKR");
            std::future::pending::<()>().await; // hold the swap; never lock
            unreachable!()
        }
        alice::run(swap, FixedRate::default()).await
    });

    // ---- Bob (taker): cli swarm + event loop, dialing Alice. ----
    let bob_db_path = scratch.join("bob.sqlite");
    let bob_db = open_db(&bob_db_path).await?;
    let bob_identity = bob_seed.derive_libp2p_identity();

    let behaviour = cli::Behaviour::new(
        env_config,
        bob_btc.clone(),
        bob_identity.clone(),
        XmrBtcNamespace::Testnet,
        Vec::new(), // rendezvous nodes
        bob_db.clone(), // wormhole store (SqliteDatabase impls WormholeStore)
    );
    let (mut bob_swarm, tor_priority_tracker) = swarm::cli(bob_identity.clone(), None, behaviour)
        .await
        .context("build bob swarm")?;
    bob_swarm.add_peer_address(alice_peer_id, alice_addr.clone());

    let (bob_event_loop, mut bob_handle) =
        cli::EventLoop::new(bob_swarm, bob_db.clone(), None, tor_priority_tracker)
            .context("build bob event loop")?;
    tokio::spawn(bob_event_loop.run());

    let swap_id = Uuid::new_v4();
    bob_db.insert_peer_id(swap_id, alice_peer_id).await.context("insert peer id")?;
    let bob_swap_handle = bob_handle
        .swap_handle(alice_peer_id, swap_id)
        .await
        .context("bob swap handle")?;

    let monero_pool: MoneroAddressPool = swap_serde::monero::address::parse(DUMMY_XMR_ADDRESS)
        .context("parse dummy monero address")?
        .into();
    let bob_change = bob_btc.new_address().await.context("bob change address")?;

    let bob_swap = bob::Swap::new(
        bob_db.clone(),
        swap_id,
        bob_btc.clone(),
        env_config,
        bob_swap_handle,
        monero_pool,
        bob_xkr_receive,
        bob_change,
        btc_amount,
        Amount::from_sat(1000), // fixed tx_lock fee
    );

    println!("[l3] starting swap {swap_id} for {btc_amount}");
    let bob_join = tokio::spawn(bob::run(bob_swap));

    // ---- Wait for both parties to finish, within the overall budget. ----
    let budget = Duration::from_secs(timeout_secs);

    let bob_state = match timeout(budget, bob_join).await {
        Ok(join) => join.context("bob task panicked")?.context("bob swap errored")?,
        Err(_) => bail!("bob swap timed out after {timeout_secs}s"),
    };
    println!("[l3] bob terminal state: {bob_state:?}");

    if refund_mode {
        // Alice is intentionally idle; stop her task, then assert Bob reclaimed
        // his BTC. Any of the refund terminals counts as a safe recovery.
        alice_join.abort();
        match bob_state {
            bob::BobState::BtcRefunded(_)
            | bob::BobState::BtcEarlyRefunded(_)
            | bob::BobState::BtcPartiallyRefunded(_) => {}
            other => bail!("bob did not reach a refund terminal; ended in {other:?}"),
        }
        let _ = tokio::fs::remove_dir_all(&scratch).await;
        println!("[l3] REFUND SAFETY PATH PASSED (bob reclaimed his BTC)");
        return Ok(());
    }

    let alice_state = match timeout(budget, alice_join).await {
        Ok(join) => join.context("alice task panicked")?.context("alice swap errored")?,
        Err(_) => bail!("alice swap timed out after {timeout_secs}s"),
    };
    println!("[l3] alice terminal state: {alice_state:?}");

    match bob_state {
        bob::BobState::XmrRedeemed { .. } => {}
        other => bail!("bob did not reach XmrRedeemed (happy path); ended in {other:?}"),
    }
    match alice_state {
        alice::AliceState::BtcRedeemed => {}
        other => bail!("alice did not reach BtcRedeemed (happy path); ended in {other:?}"),
    }

    // Best-effort cleanup of scratch state.
    let _ = tokio::fs::remove_dir_all(&scratch).await;

    println!("[l3] TWO-PARTY BTC<->XKR SWAP PASSED");
    Ok(())
}
