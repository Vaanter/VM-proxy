use crate::sqlite_proxy::main_loop;
use config::Config;
use once_cell::sync::Lazy;
use sqlite::SqliteConnection;
use sqlx::{Connection, sqlite};
use std::sync::Arc;
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_attributes::instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

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
    .with_target(true)
    .with_filter(EnvFilter::new("VM_proxy=debug"));

  Registry::default().with(fmt_layer).init();

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(run())
}

#[instrument]
async fn run() {
  let db_path = CONFIG.get_string("db_path").unwrap();
  let db = SqliteConnection::connect(&db_path).await.unwrap();
  let target = CONFIG.get_string("target").unwrap();
  let host = CONFIG.get_bool("host").unwrap();
  let handle = tokio::spawn(async move {
    main_loop(target, Arc::new(Mutex::new(db)), host)
      .await
      .unwrap();
  });
  match ctrl_c().await {
    Ok(()) => {
      info!("Ctrl-c received!");
      handle.abort();
      // if let Err(e) = timeout(Duration::from_secs(5), handle).await {
      //   error!("Task failed to exit in time");
      // }
    }
    Err(e) => error!("Ctrl-c signal error! {e}"),
  }
}
