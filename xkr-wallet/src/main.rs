//! CLI + long-running entry point for the XKR wallet client.
//!
//! The wallet app (Aesir/yggdrasil-wallet) spawns this binary as a child
//! process in `serve` mode; it connects to the local XKR wallet JSON-RPC
//! service and stays alive as the swap engine's XKR-side worker. The one-shot
//! subcommands are for testing the boundary by hand.
//!
//! Usage:
//!   xkr-wallet [--rpc-url URL] serve
//!   xkr-wallet [--rpc-url URL] ping
//!   xkr-wallet [--rpc-url URL] encode-address <spendPub> <viewPub>
//!   xkr-wallet [--rpc-url URL] watch-for-lock <address> <viewSecret> <amount> [timeoutMs]
//!   xkr-wallet [--rpc-url URL] sweep <spendSecret> <viewSecret> <dest> [fee]

use std::time::Duration;

use anyhow::{Result, anyhow};
use xkr_wallet::XkrWalletClient;

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:40000";

struct Args {
    rpc_url: String,
    command: Option<String>,
    rest: Vec<String>,
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut rpc_url = DEFAULT_RPC_URL.to_string();
    let mut positional = Vec::new();
    let mut i = 1;
    while i < raw.len() {
        if raw[i] == "--rpc-url" {
            i += 1;
            if i < raw.len() {
                rpc_url = raw[i].clone();
            }
        } else {
            positional.push(raw[i].clone());
        }
        i += 1;
    }
    let command = positional.first().cloned();
    let rest = positional.split_first().map(|(_, r)| r.to_vec()).unwrap_or_default();
    Args { rpc_url, command, rest }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();
    let client = XkrWalletClient::new(args.rpc_url.clone());

    match args.command.as_deref() {
        Some("ping") => {
            println!("{}", client.ping().await?);
        }
        Some("encode-address") => {
            let spend = args.rest.first().ok_or_else(|| anyhow!("spendPub required"))?;
            let view = args.rest.get(1).ok_or_else(|| anyhow!("viewPub required"))?;
            println!("{}", client.encode_address(spend, view).await?);
        }
        Some("watch-for-lock") => {
            let address = args.rest.first().ok_or_else(|| anyhow!("address required"))?;
            let view_secret = args.rest.get(1).ok_or_else(|| anyhow!("viewSecret required"))?;
            let amount: u64 = args.rest.get(2).ok_or_else(|| anyhow!("amount required"))?.parse()?;
            let timeout_ms = args.rest.get(3).and_then(|s| s.parse().ok());
            client.watch_for_lock(address, view_secret, amount, timeout_ms).await?;
            println!("detected");
        }
        Some("sweep") => {
            let spend_secret = args.rest.first().ok_or_else(|| anyhow!("spendSecret required"))?;
            let view_secret = args.rest.get(1).ok_or_else(|| anyhow!("viewSecret required"))?;
            let dest = args.rest.get(2).ok_or_else(|| anyhow!("dest required"))?;
            let fee = args.rest.get(3).and_then(|s| s.parse().ok());
            println!("{}", client.sweep(spend_secret, view_secret, dest, fee).await?);
        }
        Some("serve") | None => serve(&client, &args.rpc_url).await,
        Some(other) => return Err(anyhow!("unknown command: {other}")),
    }
    Ok(())
}

/// Long-running mode: wait for the XKR wallet RPC service to come up, announce
/// readiness on stdout (the wallet app watches for this), then health-ping
/// until the process is killed.
async fn serve(client: &XkrWalletClient, rpc_url: &str) {
    loop {
        match client.ping().await {
            Ok(_) => {
                println!("xkr-wallet: connected to RPC at {rpc_url}");
                break;
            }
            Err(err) => {
                eprintln!("xkr-wallet: waiting for RPC at {rpc_url}: {err}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        if let Err(err) = client.ping().await {
            eprintln!("xkr-wallet: RPC ping failed: {err}");
        }
    }
}
