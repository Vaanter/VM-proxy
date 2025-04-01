use crate::utils::{connect_downstream, create_upstream_listener};
use anyhow::bail;
use rusqlite::blob::ZeroBlob;
use rusqlite::{Connection, DatabaseName, params, params_from_iter};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::yield_now;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

pub async fn main_loop(
  address: String,
  db: Arc<Mutex<Connection>>,
  host: bool,
) -> anyhow::Result<()> {
  let (connection_sender, mut connection_receiver) = channel::<(TcpStream, usize)>(1024);
  let creator_handle = match host {
    true => tokio::spawn({
      let db = db.clone();
      async move { setup_host(address, connection_sender, db.clone()).await }
    }),
    false => tokio::spawn({
      let db = db.clone();
      let connection_sender = connection_sender.clone();
      async move { setup_client(address, connection_sender, db.clone()).await }
    }),
  };

  loop {
    let connection = connection_receiver.recv().await.unwrap();
    info!("Connection ID received: {}", connection.1);
    tokio::spawn({
      let db = db.clone();
      async move {
        connection_loop(db, connection, host)
          .await
          .expect("conn loop");
      }
    });
  }

  Ok(())
}

async fn connection_loop(
  db: Arc<Mutex<Connection>>,
  connection: (TcpStream, usize),
  host: bool,
) -> anyhow::Result<()> {
  let table_name = match host {
    true => ("upstream".to_string(), "downstream".to_string()),
    false => ("downstream".to_string(), "upstream".to_string()),
  };
  let tcp = connection.0;
  let rowid = connection.1;

  let (data_sender, data_receiver) = channel::<Vec<u8>>(4096);
  let db_task = tokio::spawn({
    let db = db.clone();
    let table_name = table_name.clone();
    async move {
      debug!("Starting db loop");
      db_loop(table_name.1, db, rowid, data_sender).await;
    }
  });

  let writer_task = tokio::spawn({
    async move {
      debug!("Starting writer loop");
      writer_loop(db, (tcp, rowid), table_name.0, data_receiver).await;
    }
  });

  // db_task.await?;
  // writer_task.await?;

  sleep(Duration::from_secs(3600)).await;

  Ok(())
}

async fn db_loop(
  table_name: String,
  db: Arc<Mutex<Connection>>,
  connection_id: usize,
  data_sender: Sender<Vec<u8>>,
) {
  loop {
    let table_name = table_name.clone();
    let db = db.clone();
    let connection_id = connection_id.clone();
    let values = tokio::task::spawn_blocking(move || {
      let mut values = BTreeMap::new();
      let conn_lock = match db.try_lock() {
        Ok(conn_lock) => conn_lock,
        Err(e) => {
          //warn!("Failed to acquire database connection: {}", e);
          return values;
        }
      };
      match conn_lock.prepare(&format!(
        "SELECT rowid FROM {} WHERE was_read = 0 AND connection = (?1) ORDER BY rowid",
        &table_name
      )) {
        Ok(mut statement) => {
          let mut rows = statement
            .query(params![connection_id])
            .expect("query failed");

          while let Some(row) = rows.next().expect("no row returned") {
            debug!("Found row {:?}", row);
            let rowid: i64 = row.get(0).expect("rowid");
            let mut data = conn_lock
              .blob_open(DatabaseName::Main, &table_name, "data", rowid, true)
              .expect("Failed to open downstream blob");
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).expect("Failed to read data");
            values.insert(rowid, buf);
          }
        }
        Err(e) => {
          error!("Failed to prepare statement: {}", e);
          return values;
        }
      };

      if values.is_empty() {
        return values;
      }

      debug!("Marking rows as read");
      let placeholders: String = std::iter::repeat("?")
        .take(values.len())
        .collect::<Vec<_>>()
        .join(",");
      let keys = params_from_iter(values.keys());
      conn_lock
        .execute(
          &format!(
            "UPDATE {} SET was_read = 1 WHERE rowid IN ({});",
            &table_name, placeholders
          ),
          keys,
        )
        .expect("Failed to update downstream");
      values
    })
    .await
    .unwrap();
    yield_now().await;

    for value in values {
      match data_sender.send(value.1).await {
        Ok(_) => {
          info!("Successfully sent data to writer");
        }
        Err(e) => {
          error!("Failed to send data from DB: {}", e);
          continue;
        }
      }
    }
  }
}

async fn writer_loop(
  db: Arc<Mutex<Connection>>,
  connection: (TcpStream, usize),
  table_name: String,
  mut data_receiver: Receiver<Vec<u8>>,
) {
  let mut tcp = connection.0;
  let connection_id = connection.1;
  let mut tcp_buffer = [0u8; 1024];

  loop {
    let read_tcp = tcp.read(&mut tcp_buffer);
    let read_db = data_receiver.recv();

    tokio::select! {
      data = read_db => {
        debug!("Received from DB: {:?}", data.unwrap());
        // if let Err(e) = tcp.write_all(&data.unwrap()).await {
        //   error!("Failed to write to {}: {}", table_name, e);
        // }
      },
      n = read_tcp => {
        let n = n.unwrap();
        if n == 0 {
          info!("Connection closed");
          break;
        }
        debug!("Read data from tcp: '{:?}'", &tcp_buffer[..n]);
        let lock = db.lock().await;
        debug!("Writing data to DB");
        let rowid: i64 = match lock.query_row(
          &format!("INSERT INTO {} (connection, data, was_read) VALUES (?1, ?2, 1) RETURNING rowid", &table_name),
          params![connection_id, ZeroBlob(n as i32)],
          |row| row.get(0)) {
          Ok(row) => row,
          Err(e) => {
            error!("Failed to insert data to DB: {}", e);
            continue;
          }
        };
        debug!("Opening blob for rowid {}", rowid);
        let mut blob = match lock.blob_open(DatabaseName::Main, &table_name, "data", rowid, false) {
          Ok(blob) => blob,
          Err(e) => {
            error!("Failed to open blob: {}", e);
            continue;
          }
        };

        debug!("Writing blob to DB");
        if let Err(e) = blob.write_all(&tcp_buffer[0..n]) {
          error!("Failed to write blob to DB: {}", e);
        }

        debug!("Setting new data row as not read");
        if let Err(e) = lock.execute(&format!("UPDATE {} SET was_read = 0 WHERE rowid = ?1", &table_name), params![rowid]) {
          error!("Failed to update data to DB: {}", e);
        }
      }
    }
  }
}

async fn setup_host(
  address: String,
  connection_sender: Sender<(TcpStream, usize)>,
  db: Arc<Mutex<Connection>>,
) -> anyhow::Result<()> {
  let listener = match create_upstream_listener(&address).await? {
    Some(listener) => listener,
    None => bail!("Failed to create upstream listener"),
  };

  loop {
    match listener.accept().await {
      Ok((client, client_address)) => {
        info!("Client connected: {}", client_address);
        let connection_id = match notify_new_host(db.clone()).await {
          Ok(connection_id) => connection_id,
          Err(e) => {
            error!(
              "Failed to register new host for connection {}: {}",
              &client_address, e
            );
            continue;
          }
        };
        info!(
          "Created connection {} for client: {}",
          connection_id, client_address
        );
        if let Err(e) = connection_sender.send((client, connection_id)).await {
          error!("Failed to finish setup for upstream {}", e);
        }
      }
      Err(e) => {
        error!("Failed to establish client connection: {}", e);
      }
    }
  }
}

async fn notify_new_host(db: Arc<Mutex<Connection>>) -> rusqlite::Result<usize> {
  db.lock().await.query_row(
    "INSERT INTO connections default values RETURNING connection",
    params![],
    |row| row.get(0),
  )
}

async fn setup_client(
  address: String,
  connection_sender: Sender<(TcpStream, usize)>,
  db: Arc<Mutex<Connection>>,
) -> anyhow::Result<()> {
  loop {
    let connection_id = match poll_connection_id(db.clone()) {
      Some(connection_id) => connection_id,
      None => continue,
    };

    let target = match connect_downstream(&address).await {
      Ok(tcp) => tcp,
      Err(e) => {
        error!("Failed to establish connection to upstream: {}", e);
        continue;
      }
    };

    if let Err(e) = connection_sender.send((target, connection_id)).await {
      error!("Failed to finish setup for downstream connection: {}", e);
    }
    break;
  }

  Ok(())
}

fn poll_connection_id(db: Arc<Mutex<Connection>>) -> Option<usize> {
  match db.try_lock() {
    Ok(lock) => {
      match lock.query_row(
        r#"
        SELECT connection FROM connections
        WHERE used = 0
        ORDER BY rowid
        LIMIT 1;"#,
        [],
        |row| row.get(0),
      ) {
        Ok(connection) => Some(connection),
        Err(e) => {
          //trace!("Failed to get connection id: {}", e);
          None
        }
      }
    }
    Err(e) => {
      trace!("Failed to acquire database lock: {}", e);
      None
    }
  }
}
