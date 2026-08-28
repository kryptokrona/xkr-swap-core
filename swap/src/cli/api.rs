pub mod request;
pub mod tauri_bindings;

use crate::cli::api::tauri_bindings::ContextStatus;
use crate::cli::command::{Bitcoin, Monero};
use crate::common::tor::{bootstrap_tor_client, create_tor_client};
use crate::common::tracing_util::Format;
use crate::database::{AccessMode, open_db};
use crate::network::rendezvous::XmrBtcNamespace;
use crate::protocol::Database;
use crate::seed::Seed;
use crate::{common, monero};
use anyhow::{Context as AnyContext, Error, Result, bail};
use arti_client::TorClient;
use futures::future::try_join_all;
use libp2p::{Multiaddr, PeerId};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use swap_env::env::{Config as EnvConfig, GetConfig, Mainnet, Testnet};
use swap_fs::system_data_dir;
use tauri_bindings::{MoneroNodeConfig, TauriBackgroundProgress, TauriEmitter, TauriHandle};
use tokio::sync::{Mutex as TokioMutex, RwLock, broadcast, broadcast::Sender};
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;
use tor_rtcompat::tokio::TokioRustlsRuntime;
use uuid::Uuid;

use super::watcher::Watcher;

static START: Once = Once::new();

mod config {
    use super::*;

    #[derive(Clone, PartialEq, Debug)]
    pub struct Config {
        pub(super) namespace: XmrBtcNamespace,
        pub env_config: EnvConfig,
        pub(super) seed: Option<Seed>,
        pub(super) json: bool,
        pub(super) log_dir: PathBuf,
        pub(super) data_dir: PathBuf,
        pub(super) is_testnet: bool,
    }

    impl Config {
        pub fn for_harness(seed: Seed, env_config: EnvConfig) -> Self {
            let data_dir =
                super::data::data_dir_from(None, false).expect("Could not find data directory");
            let log_dir = data_dir.join("logs"); // not used in production

            Self {
                namespace: XmrBtcNamespace::from_is_testnet(false),
                env_config,
                seed: seed.into(),
                json: false,
                is_testnet: false,
                data_dir,
                log_dir,
            }
        }
    }

    pub(super) fn env_config_from(testnet: bool) -> EnvConfig {
        if testnet {
            Testnet::get_config()
        } else {
            Mainnet::get_config()
        }
    }
}

pub use config::Config;

mod swap_lock {
    use super::*;

    #[derive(Default)]
    pub struct PendingTaskList(TokioMutex<Vec<JoinHandle<()>>>);

    impl PendingTaskList {
        pub async fn spawn<F, T>(&self, future: F)
        where
            F: Future<Output = T> + Send + 'static,
            T: Send + 'static,
        {
            let handle = tokio::spawn(async move {
                let _ = future.await;
            });

            self.0.lock().await.push(handle);
        }

        pub async fn wait_for_tasks(&self) -> Result<()> {
            let tasks = {
                // Scope for the lock, to avoid holding it for the entire duration of the async block
                let mut guard = self.0.lock().await;
                guard.drain(..).collect::<Vec<_>>()
            };

            try_join_all(tasks).await?;

            Ok(())
        }
    }

    /// The `SwapLock` manages the state of the current swap, ensuring that only one swap can be active at a time.
    /// It includes:
    /// - A lock for the current swap (`current_swap`)
    /// - A broadcast channel for suspension signals (`suspension_trigger`)
    ///
    /// The `SwapLock` provides methods to acquire and release the swap lock, and to listen for suspension signals.
    /// This ensures that swap operations do not overlap and can be safely suspended if needed.
    pub struct SwapLock {
        current_swap: RwLock<Option<Uuid>>,
        suspension_trigger: Sender<()>,
    }

    impl SwapLock {
        pub fn new() -> Self {
            let (suspension_trigger, _) = broadcast::channel(10);
            SwapLock {
                current_swap: RwLock::new(None),
                suspension_trigger,
            }
        }

        pub async fn listen_for_swap_force_suspension(&self) -> Result<(), Error> {
            let mut listener = self.suspension_trigger.subscribe();
            let event = listener.recv().await;
            match event {
                Ok(_) => Ok(()),
                Err(e) => {
                    tracing::error!("Error receiving swap suspension signal: {}", e);
                    bail!(e)
                }
            }
        }

        pub async fn acquire_swap_lock(&self, swap_id: Uuid) -> Result<(), Error> {
            let mut current_swap = self.current_swap.write().await;
            if current_swap.is_some() {
                bail!("There already exists an active swap lock");
            }

            tracing::debug!(swap_id = %swap_id, "Acquiring swap lock");
            *current_swap = Some(swap_id);
            Ok(())
        }

        pub async fn get_current_swap_id(&self) -> Option<Uuid> {
            *self.current_swap.read().await
        }

        /// Sends a signal to suspend all ongoing swap processes.
        ///
        /// This function performs the following steps:
        /// 1. Triggers the suspension by sending a unit `()` signal to all listeners via `self.suspension_trigger`.
        /// 2. Polls the `current_swap` state every 50 milliseconds to check if it has been set to `None`, indicating that the swap processes have been suspended and the lock released.
        /// 3. If the lock is not released within 10 seconds, the function returns an error.
        ///
        /// If we send a suspend signal while no swap is in progress, the function will not fail, but will return immediately.
        ///
        /// # Returns
        /// - `Ok(())` if the swap lock is successfully released.
        /// - `Err(Error)` if the function times out waiting for the swap lock to be released.
        ///
        /// # Notes
        /// The 50ms polling interval is considered negligible overhead compared to the typical time required to suspend ongoing swap processes.
        pub async fn send_suspend_signal(&self) -> Result<(), Error> {
            const TIMEOUT: u64 = 10_000;
            const INTERVAL: u64 = 50;

            let _ = self.suspension_trigger.send(())?;

            for _ in 0..(TIMEOUT / INTERVAL) {
                if self.get_current_swap_id().await.is_none() {
                    return Ok(());
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(INTERVAL)).await;
            }

            bail!("Timed out waiting for swap lock to be released");
        }

        pub async fn release_swap_lock(&self) -> Result<Uuid, Error> {
            let mut current_swap = self.current_swap.write().await;
            if let Some(swap_id) = current_swap.as_ref() {
                tracing::debug!(swap_id = %swap_id, "Releasing swap lock");

                let prev_swap_id = *swap_id;
                *current_swap = None;
                drop(current_swap);
                Ok(prev_swap_id)
            } else {
                bail!("There is no current swap lock to release");
            }
        }
    }

    impl Default for SwapLock {
        fn default() -> Self {
            Self::new()
        }
    }
}

pub use swap_lock::{PendingTaskList, SwapLock};

mod context {
    use super::*;
    use crate::cli::EventLoopHandle;

    /// Combined state for the EventLoop and its background task
    /// These must always exist together
    pub struct EventLoopState {
        pub handle: EventLoopHandle,
        #[allow(dead_code)]
        pub task: JoinHandle<()>,
    }

    impl EventLoopState {
        pub fn new(handle: EventLoopHandle, task: JoinHandle<()>) -> Self {
            Self { handle, task }
        }
    }

    /// Holds shared data for different parts of the CLI.
    ///
    /// Some components are optional, allowing initialization of only necessary parts.
    /// For example, the `history` command doesn't require wallet initialization.
    ///
    /// Components are wrapped in Arc<RwLock> to allow independent initialization and cloning.
    #[derive(Clone)]
    pub struct Context {
        pub db: Arc<RwLock<Option<Arc<dyn Database + Send + Sync>>>>,
        pub swap_lock: Arc<SwapLock>,
        pub config: Arc<RwLock<Option<Config>>>,
        pub tasks: Arc<PendingTaskList>,
        pub tauri_handle: Option<TauriHandle>,
        pub(super) bitcoin_wallet: Arc<RwLock<Option<Arc<bitcoin_wallet::Wallet>>>>,
        pub(super) tor_client: Arc<RwLock<Option<Arc<TorClient<TokioRustlsRuntime>>>>>,
        pub(super) event_loop_state: Arc<RwLock<Option<EventLoopState>>>,
    }

    impl Context {
        pub fn new_with_tauri_handle(tauri_handle: TauriHandle) -> Self {
            Self::new(Some(tauri_handle))
        }

        pub fn new_without_tauri_handle() -> Self {
            Self::new(None)
        }

        /// Creates an empty Context with only the swap_lock and tasks initialized
        fn new(tauri_handle: Option<TauriHandle>) -> Self {
            Self {
                db: Arc::new(RwLock::new(None)),
                swap_lock: Arc::new(SwapLock::new()),
                config: Arc::new(RwLock::new(None)),
                tasks: Arc::new(PendingTaskList::default()),
                tauri_handle,
                bitcoin_wallet: Arc::new(RwLock::new(None)),
                tor_client: Arc::new(RwLock::new(None)),
                event_loop_state: Arc::new(RwLock::new(None)),
            }
        }

        pub async fn status(&self) -> ContextStatus {
            ContextStatus {
                bitcoin_wallet_available: self.try_get_bitcoin_wallet().await.is_ok(),
                // XKR port: no Monero wallet in the engine.
                monero_wallet_available: false,
                database_available: self.try_get_db().await.is_ok(),
                tor_available: self.try_get_tor_client().await.is_ok(),
            }
        }

        /// Get the Bitcoin wallet, returning an error if not initialized
        pub async fn try_get_bitcoin_wallet(&self) -> Result<Arc<bitcoin_wallet::Wallet>> {
            self.bitcoin_wallet
                .read()
                .await
                .clone()
                .context("Bitcoin wallet not initialized")
        }


        /// Get the database, returning an error if not initialized
        pub async fn try_get_db(&self) -> Result<Arc<dyn Database + Send + Sync>> {
            self.db
                .read()
                .await
                .clone()
                .context("Database not initialized")
        }

        /// Get the config, returning an error if not initialized
        pub async fn try_get_config(&self) -> Result<Config> {
            self.config
                .read()
                .await
                .clone()
                .context("Config not initialized")
        }

        /// Get the Tor client, returning an error if not initialized
        pub async fn try_get_tor_client(&self) -> Result<Arc<TorClient<TokioRustlsRuntime>>> {
            self.tor_client
                .read()
                .await
                .clone()
                .context("Tor client not initialized")
        }

        /// Get the event loop handle, returning an error if not initialized
        pub async fn try_get_event_loop_handle(&self) -> Result<EventLoopHandle> {
            self.event_loop_state
                .read()
                .await
                .as_ref()
                .map(|state| state.handle.clone())
                .context("Event loop not initialized")
        }

        pub async fn for_harness(
            seed: Seed,
            env_config: EnvConfig,
            db_path: PathBuf,
            bob_bitcoin_wallet: Arc<bitcoin_wallet::Wallet>,
        ) -> Self {
            let config = Config::for_harness(seed, env_config);
            let db = open_db(db_path, AccessMode::ReadWrite, None)
                .await
                .expect("Could not open sqlite database");

            Self {
                bitcoin_wallet: Arc::new(RwLock::new(Some(bob_bitcoin_wallet))),
                config: Arc::new(RwLock::new(Some(config))),
                db: Arc::new(RwLock::new(Some(db))),
                swap_lock: SwapLock::new().into(),
                tasks: PendingTaskList::default().into(),
                tauri_handle: None,
                tor_client: Arc::new(RwLock::new(None)),
                event_loop_state: Arc::new(RwLock::new(None)),
            }
        }

        pub fn cleanup(&self) -> Result<()> {
            // TODO: close all monero wallets
            // call store(..) on all wallets

            // TODO: This doesn't work because "there is no reactor running, must be called from the context of a Tokio 1.x runtime"
            // let monero_manager = self.monero_manager.clone();
            // tokio::spawn(async move {
            //     if let Some(monero_manager) = monero_manager {
            //         let wallet = monero_manager.main_wallet().await;
            //         wallet.store(None).await;
            //     }
            // });

            Ok(())
        }

        pub async fn bitcoin_wallet(&self) -> Option<Arc<bitcoin_wallet::Wallet>> {
            self.bitcoin_wallet.read().await.clone()
        }

    }

    impl fmt::Debug for Context {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "")
        }
    }
}

pub use context::Context;

mod builder {
    use super::*;
    use crate::cli::api::context::EventLoopState;

    /// A convenient builder struct for [`Context`].
    #[must_use = "ContextBuilder must be built to be useful"]
    pub struct ContextBuilder {
        bitcoin: Option<Bitcoin>,
        data: Option<PathBuf>,
        is_testnet: bool,
        json: bool,
        tor: bool,
        tauri_handle: Option<TauriHandle>,
        rendezvous_points: Vec<(PeerId, Vec<Multiaddr>)>,
    }

    impl ContextBuilder {
        /// Start building a context
        pub fn new(is_testnet: bool) -> Self {
            if is_testnet {
                Self::testnet()
            } else {
                Self::mainnet()
            }
        }

        /// Basic builder with default options for mainnet
        pub fn mainnet() -> Self {
            ContextBuilder {
                bitcoin: None,
                data: None,
                is_testnet: false,
                json: false,
                tor: false,
                tauri_handle: None,
                rendezvous_points: Vec::new(),
            }
        }

        /// Basic builder with default options for testnet
        pub fn testnet() -> Self {
            let mut builder = Self::mainnet();
            builder.is_testnet = true;
            builder
        }

        /// Configures the Context to initialize a Bitcoin wallet with the given configuration.
        pub fn with_bitcoin(mut self, bitcoin: impl Into<Option<Bitcoin>>) -> Self {
            self.bitcoin = bitcoin.into();
            self
        }

        /// Attach a handle to Tauri to the Context for emitting events etc.
        pub fn with_tauri(mut self, tauri_handle: impl Into<Option<TauriHandle>>) -> Self {
            self.tauri_handle = tauri_handle.into();
            self
        }

        /// Configures where the data and logs are saved in the filesystem
        pub fn with_data_dir(mut self, data: impl Into<Option<PathBuf>>) -> Self {
            self.data = data.into();
            self
        }

        /// Set logging format to json (default false)
        pub fn with_json(mut self, json: bool) -> Self {
            self.json = json;
            self
        }

        /// Whether to initialize a Tor client (default false)
        pub fn with_tor(mut self, tor: bool) -> Self {
            self.tor = tor;
            self
        }

        pub fn with_rendezvous_points(
            mut self,
            rendezvous_points: Vec<(PeerId, Vec<Multiaddr>)>,
        ) -> Self {
            self.rendezvous_points = rendezvous_points;
            self
        }

        /// Initializes the context by populating it with all configured components.
        ///
        /// Context fields are set as early as possible for availability to other parts of the system.
        pub async fn build(self, context: Arc<Context>) -> Result<()> {
            let eigenwallet_data_dir = &eigenwallet_data::new(self.is_testnet)?;
            let base_data_dir = &data::data_dir_from(self.data, self.is_testnet)?;
            let log_dir = base_data_dir.join("logs");
            let env_config = config::env_config_from(self.is_testnet);

            // Initialize logging
            let format = if self.json { Format::Json } else { Format::Raw };

            START.call_once(|| {
                let _ = common::tracing_util::init(
                    format,
                    log_dir.clone(),
                    self.tauri_handle.clone(),
                    true,
                );
                tracing::info!(
                    binary = "cli",
                    version = env!("CARGO_PKG_VERSION"),
                    os = std::env::consts::OS,
                    arch = std::env::consts::ARCH,
                    "Setting up context"
                );
            });

            // Initialize the Tor client. The XKR build has no Monero RPC pool or
            // Monero wallet database — the taker's XKR side is the external XKR
            // wallet service.
            let unbootstrapped_tor_client = if self.tor {
                match create_tor_client(&base_data_dir).await.inspect_err(|err| {
                    tracing::warn!(%err, "Failed to create Tor client. We will continue without Tor");
                }) {
                    Ok(client) => Some(client),
                    Err(_) => None,
                }
            } else {
                tracing::warn!("Internal Tor client not enabled, skipping initialization");
                None
            };

            // Bootstrap the Tor client in the background; awaited later so the
            // context waits for Tor to finish bootstrapping.
            let bootstrap_tor_client_task = AbortOnDropHandle::new(tokio::spawn({
                let unbootstrapped_tor_client = unbootstrapped_tor_client.clone();
                let tauri_handle = self.tauri_handle.clone();

                async move {
                    if let Some(tor_client) = unbootstrapped_tor_client {
                        bootstrap_tor_client(tor_client.clone(), tauri_handle.clone())
                            .await
                            .inspect_err(|err| {
                                tracing::warn!(%err, "Failed to bootstrap Tor client. It will remain unbootstrapped");
                            })
                            .ok();
                    }
                }
            }));

            *context.tor_client.write().await = unbootstrapped_tor_client.clone();

            // XKR port: no Monero wallet. The taker's identity and data directory
            // are derived from the engine seed (a seed file), its XKR funds live in
            // the external XKR wallet service, and `monero_manager` stays `None`.
            let seed = Seed::from_file_or_generate(base_data_dir.as_path())
                .await
                .context("Failed to read or generate the engine seed")?;

            // Identity = the seed-derived libp2p peer id (stable per taker).
            let identity = seed
                .derive_libp2p_identity()
                .public()
                .to_peer_id()
                .to_string();

            let data_dir = base_data_dir.join("identities").join(&identity);

            swap_fs::ensure_directory_exists(&data_dir)
                .context("Failed to create identity directory")?;

            tracing::info!(
                %identity,
                data_dir = %data_dir.display(),
                "Using seed-derived identity data directory"
            );

            // Initialize config
            *context.config.write().await = Some(Config {
                namespace: XmrBtcNamespace::from_is_testnet(self.is_testnet),
                env_config,
                seed: seed.clone().into(),
                json: self.json,
                is_testnet: self.is_testnet,
                data_dir: data_dir.clone(),
                log_dir: log_dir.clone(),
            });

            // Initialize swap database
            let db = async {
                let database_progress_handle = self
                    .tauri_handle
                    .new_background_process_with_initial_progress(
                        TauriBackgroundProgress::OpeningDatabase,
                        (),
                    );

                let db = open_db(
                    data_dir.join("sqlite"),
                    AccessMode::ReadWrite,
                    self.tauri_handle.clone(),
                )
                .await?;

                database_progress_handle.finish();

                *context.db.write().await = Some(db.clone());

                Ok::<_, Error>(db)
            }
            .await?;

            let tauri_handle = &self.tauri_handle.clone();

            // Initialize Bitcoin wallet
            let bitcoin_wallet = async {
                let wallet = match self.bitcoin {
                    Some(bitcoin) => {
                        let (urls, target_block) = bitcoin.apply_defaults(self.is_testnet)?;

                        let bitcoin_progress_handle = tauri_handle
                            .new_background_process_with_initial_progress(
                                TauriBackgroundProgress::OpeningBitcoinWallet,
                                (),
                            );

                        let wallet = wallet::init_bitcoin_wallet(
                            urls,
                            &seed,
                            &data_dir,
                            env_config,
                            target_block,
                            self.tauri_handle.clone(),
                        )
                        .await?;

                        bitcoin_progress_handle.finish();

                        Some(Arc::new(wallet))
                    }
                    None => None,
                };

                *context.bitcoin_wallet.write().await = wallet.clone();

                Ok::<_, Error>(wallet)
            }
            .await?;

            // If we have a bitcoin wallet and a tauri handle, we start a background task
            if let Some(wallet) = bitcoin_wallet.clone() {
                if self.tauri_handle.is_some() {
                    let watcher = Watcher::new(
                        wallet,
                        db.clone(),
                        self.tauri_handle.clone(),
                        context.swap_lock.clone(),
                    );
                    tokio::spawn(watcher.run());
                }
            }

            // Initialize persistent EventLoop
            if let Some(wallet) = bitcoin_wallet.clone() {
                let namespace = XmrBtcNamespace::from_is_testnet(self.is_testnet);
                let tor_client_for_swarm = unbootstrapped_tor_client.clone();
                let db_for_swarm = db.clone();

                let rendezvous_points = self.rendezvous_points;
                let rendezvous_peer_ids: Vec<PeerId> =
                    rendezvous_points.iter().map(|(p, _)| *p).collect();

                let behaviour = crate::cli::Behaviour::new(
                    env_config,
                    wallet.clone(),
                    seed.derive_libp2p_identity(),
                    namespace,
                    rendezvous_peer_ids.clone(),
                    db.clone(),
                );

                let (mut swarm, tor_priority_tracker) = crate::network::swarm::cli(
                    seed.derive_libp2p_identity(),
                    tor_client_for_swarm,
                    behaviour,
                )
                .await?;

                if let Some(tor_priority_tracker) = &tor_priority_tracker {
                    for peer_id in &rendezvous_peer_ids {
                        tor_priority_tracker.mark_high_priority(*peer_id);
                    }
                }

                // Add addresses of rendezvous points to the swarm
                for (peer_id, addrs) in rendezvous_points {
                    for addr in addrs {
                        swarm.add_peer_address(peer_id, addr);
                    }
                }

                // Add addresses of peers that we have previously connected to to the swarm
                for (peer_id, addrs) in db.get_all_peer_addresses().await.context(
                    "Failed to retrieve peer addresses from database to insert into swarm",
                )? {
                    if let Some(tor_priority_tracker) = &tor_priority_tracker {
                        tor_priority_tracker.mark_low_priority(peer_id);
                    }
                    for addr in addrs {
                        swarm.add_peer_address(peer_id, addr);
                    }
                }

                let (event_loop, event_loop_handle) = crate::cli::EventLoop::new(
                    swarm,
                    db_for_swarm,
                    self.tauri_handle.clone(),
                    tor_priority_tracker,
                )?;

                let event_loop_task = tokio::spawn(event_loop.run());

                // TODO: We need to emit this to the frontend so that certain button can be disabled if the event loop is not yet running
                *context.event_loop_state.write().await =
                    Some(EventLoopState::new(event_loop_handle, event_loop_task));
            }

            // Wait for Tor client to fully bootstrap
            bootstrap_tor_client_task.await?;

            Ok(())
        }
    }
}

pub use builder::ContextBuilder;

mod wallet {
    use super::*;

    pub(super) async fn init_bitcoin_wallet(
        electrum_rpc_urls: Vec<String>,
        seed: &Seed,
        data_dir: &Path,
        env_config: EnvConfig,
        bitcoin_target_block: u16,
        tauri_handle_option: Option<TauriHandle>,
    ) -> Result<bitcoin_wallet::Wallet<bdk_wallet::rusqlite::Connection, bitcoin_wallet::Client>>
    {
        let mut builder = bitcoin_wallet::WalletBuilder::<Seed>::default()
            .seed(seed.clone())
            .network(env_config.bitcoin_network)
            .electrum_rpc_urls(electrum_rpc_urls)
            .persister(bitcoin_wallet::PersisterConfig::SqliteFile {
                data_dir: data_dir.to_path_buf(),
            })
            .finality_confirmations(env_config.bitcoin_finality_confirmations)
            .target_block(bitcoin_target_block)
            .sync_interval(env_config.bitcoin_sync_interval());

        if let Some(handle) = tauri_handle_option {
            builder = builder.tauri_handle(handle.clone());
        }

        let wallet = builder
            .build()
            .await
            .context("Failed to initialize Bitcoin wallet")?;

        Ok(wallet)
    }
}

pub mod data {
    use super::*;

    pub fn data_dir_from(arg_dir: Option<PathBuf>, testnet: bool) -> Result<PathBuf> {
        let base_dir = match arg_dir {
            Some(custom_base_dir) => custom_base_dir,
            None => os_default()?,
        };

        let sub_directory = if testnet { "testnet" } else { "mainnet" };

        Ok(base_dir.join(sub_directory))
    }

    fn os_default() -> Result<PathBuf> {
        Ok(system_data_dir()?.join("cli"))
    }
}

pub mod eigenwallet_data {
    use swap_fs::system_data_dir_eigenwallet;

    use super::*;

    pub fn new(testnet: bool) -> Result<PathBuf> {
        Ok(system_data_dir_eigenwallet(testnet)?)
    }
}

impl From<Monero> for Option<MoneroNodeConfig> {
    fn from(monero: Monero) -> Self {
        let _ = monero;
        None
    }
}

#[cfg(test)]
pub mod api_test {
    use super::*;

    pub const MULTI_ADDRESS: &str =
        "/ip4/127.0.0.1/tcp/9939/p2p/12D3KooWCdMKjesXMJz1SiZ7HgotrxuqhQJbP5sgBm2BwP1cqThi";
    pub const MONERO_STAGENET_ADDRESS: &str = "53gEuGZUhP9JMEBZoGaFNzhwEgiG7hwQdMCqFxiyiTeFPmkbt1mAoNybEUvYBKHcnrSgxnVWgZsTvRBaHBNXPa8tHiCU51a";
    pub const BITCOIN_TESTNET_ADDRESS: &str = "tb1qr3em6k3gfnyl8r7q0v7t4tlnyxzgxma3lressv";
    pub const MONERO_MAINNET_ADDRESS: &str = "44Ato7HveWidJYUAVw5QffEcEtSH1DwzSP3FPPkHxNAS4LX9CqgucphTisH978FLHE34YNEx7FcbBfQLQUU8m3NUC4VqsRa";
    pub const BITCOIN_MAINNET_ADDRESS: &str = "bc1qe4epnfklcaa0mun26yz5g8k24em5u9f92hy325";
    pub const SWAP_ID: &str = "ea030832-3be9-454f-bb98-5ea9a788406b";

    impl Config {
        pub async fn default(
            is_testnet: bool,
            data_dir: Option<PathBuf>,
            _debug: bool,
            json: bool,
        ) -> Self {
            let data_dir = data::data_dir_from(data_dir, is_testnet).unwrap();
            let log_dir = data_dir.clone().join("logs");
            let seed = Seed::from_file_or_generate(data_dir.as_path())
                .await
                .unwrap();
            let env_config = config::env_config_from(is_testnet);

            Self {
                namespace: XmrBtcNamespace::from_is_testnet(is_testnet),
                env_config,
                seed: seed.into(),
                json,
                is_testnet,
                data_dir,
                log_dir,
            }
        }
    }
}
