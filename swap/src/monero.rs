// XKR port: the monero-sys wallet (`monero_wallet`) re-exports were removed. Only
// the pure-Rust Monero primitives (curve/address/amount) remain, used by the
// cross-curve crypto and address encoding.
pub mod wallet_rpc;

pub use ::monero_address::MoneroAddress as Address;
pub use ::monero_address::Network;
pub use ::monero_oxide_ext::{PrivateKey, PublicKey};
pub use curve25519_dalek::scalar::Scalar;
pub use swap_core::monero::primitives::*;
