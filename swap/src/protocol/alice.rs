//! Run a BTC/XKR swap in the role of Alice.
//! Alice holds XKR and wishes to receive BTC.
use crate::asb;
use crate::protocol::Database;
pub use crate::protocol::alice::swap::*;
use bitcoin_wallet::BitcoinWallet;
use std::sync::Arc;
use swap_env::env::Config;
pub use swap_machine::alice::*;
use uuid::Uuid;

pub mod swap;

pub struct Swap {
    pub state: AliceState,
    pub event_loop_handle: asb::EventLoopHandle,
    pub bitcoin_wallet: Arc<dyn BitcoinWallet>,
    pub env_config: Config,
    pub swap_id: Uuid,
    pub db: Arc<dyn Database + Send + Sync>,
}
