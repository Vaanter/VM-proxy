use crate::proxy::Proxy;
use crate::sqlite_proxy::main_loop;
use config::Config;
use once_cell::sync::Lazy;
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod proxy;
mod sqlite_proxy;
mod utils;

/// The configuration loaded from config file
pub(crate) static CONFIG: Lazy<Config> = Lazy::new(|| {
  Config::builder()
    .add_source(config::File::with_name("config.toml"))
    // Add in settings from the environment (with a prefix of VMPROXY)
    // E.g. `VMPROXY_DEBUG=1 ./target/app` would set the `debug` key
    .add_source(config::Environment::with_prefix("VMPROXY"))
    .build()
    .unwrap()
});

fn main() {
  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(std::io::stdout)
    .with_file(false)
    .with_ansi(false)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false);
  Registry::default().with(fmt_layer).init();

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(run())
}

async fn run() {
  let db_path = CONFIG.get_string("db_path").unwrap();
  let db = Connection::open(&db_path).unwrap();
  let target = CONFIG.get_string("target").unwrap();
  let host = CONFIG.get_bool("host").unwrap();
  let handle = tokio::spawn(async move {
    main_loop(target, Arc::new(Mutex::new(db)), host).await.unwrap();
  });
  match ctrl_c().await {
    Ok(()) => {
      info!("Ctrl-c received!");
      if let Err(e) = timeout(Duration::from_secs(5), handle).await {
        error!("Task failed to exit in time");
      }
    }
    Err(e) => error!("Ctrl-c signal error! {e}"),
  }
}

async fn run_listeners() {
  let proxies = CONFIG
    .get_array("proxies")
    .expect("Config must define proxies!")
    .into_iter()
    .map(|p| p.into_table().unwrap())
    .collect::<Vec<_>>();

  let cancel = CancellationToken::new();
  let mut tasks = BTreeMap::new();

  for (index, proxy_map) in proxies.iter().enumerate() {
    let upstream = proxy_map.get("upstream").map(|u| u.to_string());
    let downstream = proxy_map.get("downstream").map(|u| u.to_string());
    match (upstream, downstream) {
      (Some(upstream), Some(downstream)) => {
        let cancel = cancel.clone();
        info!("Creating proxy {} -> {}", &upstream, &downstream);
        let proxy = Proxy::new(upstream, downstream);
        tasks.insert(
          index,
          tokio::spawn(async move {
            if let Err(e) = proxy.listener_loop(cancel.clone()).await {
              error!("Listener {} failed: {}", index, e);
            }
          }),
        );
      }
      (_, _) => {
        error!(
          "Proxy at position {} is missing required keys {{'upstream', 'downstream'}}",
          index
        );
      }
    }
  }

  match ctrl_c().await {
    Ok(()) => {
      info!("Ctrl-c received!");
      cancel.cancel();
      for handle in tasks.iter_mut() {
        if let Err(e) = timeout(Duration::from_secs(5), handle.1).await {
          error!("Listener {} failed to exit in time", handle.0);
        }
      }
    }
    Err(e) => error!("Ctrl-c signal error! {e}"),
  }
}
