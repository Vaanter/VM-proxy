use crate::utils::{connect_downstream, create_upstream_listener};
use anyhow::bail;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use tracing_attributes::instrument;

pub struct Proxy {
  upstream: String,
  downstream: String,
}

enum DuplexEvent {
  DownstreamRead(usize),
  UpstreamRead(usize),
}

impl Proxy {
  pub fn new(upstream: String, downstream: String) -> Self {
    Proxy {
      upstream,
      downstream,
    }
  }

  #[instrument(skip_all)]
  pub async fn duplex(
    mut server_session: TcpStream,
    mut client_session: TcpStream,
    cancel: CancellationToken,
  ) {
    let mut upstream_buf = [0; 1024];
    let mut downstream_buf = [0; 1024];
    loop {
      let downstream_read = server_session.read(&mut upstream_buf);
      let upstream_read = client_session.read(&mut downstream_buf);
      let event: DuplexEvent;
      select! {
          biased;
          _ = cancel.cancelled() => return,
          n = downstream_read => event
              = DuplexEvent::DownstreamRead(n.unwrap()),
          n = upstream_read => event
              = DuplexEvent::UpstreamRead(n.unwrap()),
      }
      match event {
        DuplexEvent::DownstreamRead(0) => {
          debug!("downstream session closing");
          return;
        }
        DuplexEvent::UpstreamRead(0) => {
          debug!("upstream session closing");
          return;
        }
        DuplexEvent::DownstreamRead(n) => {
          if let Err(e) = client_session.write_all(&upstream_buf[0..n]).await {
            error!("Failed to proxy data from downstream: {}", e);
          };
          if let Err(e) = client_session.flush().await {
            error!("Failed to flush downstream: {}", e);
          };
        }
        DuplexEvent::UpstreamRead(n) => {
          if let Err(e) = server_session.write_all(&downstream_buf[0..n]).await {
            error!("Failed to proxy data from upstream: {}", e);
          };
          if let Err(e) = server_session.flush().await {
            error!("Failed to flush upstream: {}", e);
          };
        }
      }
    }
  }

  pub async fn listener_loop(&self, cancel: CancellationToken) -> anyhow::Result<()> {
    let downstream_address = Arc::new(self.downstream.clone());
    let upstream_listener = create_upstream_listener(&self.upstream).await?;
    let upstream_listener = match upstream_listener {
      Some(listener) => listener,
      None => bail!("Cannot create listener for address {}", self.upstream),
    };
    loop {
      let cancel = cancel.clone();
      select! {
        biased;
        _ = cancel.cancelled() => break,
        upstream = upstream_listener.accept() => {
          let (upstream, upstream_address) = match upstream {
            Ok(upstream) => upstream,
            Err(e) => {
              error!("Failed to receive connection from upstream! {}", e);
              continue;
            }
          };
          debug!("Received connection from {}", upstream_address);
          let downstream = match connect_downstream(&*downstream_address).await {
            Ok(downstream) => downstream,
            Err(e) => {
              error!("Failed to connect to downstream {}. {}", upstream_address, e);
              continue;
            }
          };
          tokio::spawn(async move {
            Self::duplex(upstream, downstream, cancel.clone()).await;
          });
        }
      }
    }
    Ok(())
  }
}
