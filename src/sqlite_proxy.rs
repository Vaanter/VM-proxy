use crate::utils::{connect_downstream, create_upstream_listener};
use anyhow::bail;
use sqlx::{Connection, Error, Executor, Row, SqliteConnection};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::yield_now;
use tokio::time::sleep;
use tracing::{debug, error, info, trace};
use tracing_attributes::instrument;

pub async fn main_loop(
  address: String,
  db: Arc<Mutex<SqliteConnection>>,
  host: bool,
) -> anyhow::Result<()> {
  let (connection_sender, mut connection_receiver) = channel::<(TcpStream, i64)>(1024);
  let _creator_handle = match host {
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
}

#[instrument(skip_all)]
async fn connection_loop(
  db: Arc<Mutex<SqliteConnection>>,
  connection: (TcpStream, i64),
  host: bool,
) -> anyhow::Result<()> {
  let table_name = match host {
    true => ("upstream".to_string(), "downstream".to_string()),
    false => ("downstream".to_string(), "upstream".to_string()),
  };
  let tcp = connection.0;
  let rowid = connection.1;

  let (data_sender, data_receiver) = channel::<Vec<u8>>(4096);
  let _db_task = tokio::spawn({
    let db = db.clone();
    let table_name = table_name.clone();
    async move {
      debug!("Starting db loop");
      db_loop(table_name.1, db, rowid, data_sender).await;
    }
  });

  let _writer_task = tokio::spawn({
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
  db: Arc<Mutex<SqliteConnection>>,
  connection_id: i64,
  data_sender: Sender<Vec<u8>>,
) {
  loop {
    let table_name = table_name.clone();
    let db = db.clone();
    let connection_id = connection_id.clone();
    let mut conn_lock = db.lock().await;
    let mut transaction = match SqliteConnection::begin(&mut *conn_lock).await {
      Ok(transaction) => transaction,
      Err(e) => {
        error!("Failed to create transaction. {}", e);
        continue;
      }
    };
    let rows = match transaction
      .fetch_all(
        sqlx::query(&format!(
          "SELECT rowid, data FROM {} WHERE was_read = 0 AND connection = $1 ORDER BY rowid",
          &table_name
        ))
        .bind(connection_id),
      )
      .await
    {
      Ok(rows) => {
        let mut values = BTreeMap::new();
        for row in rows {
          let rowid: i64 = row.get(0);
          let data: Vec<u8> = row.get(1);
          values.insert(rowid, data);
        }
        values
      }
      Err(e) => {
        error!("Failed to select unread data. {}", e);
        continue;
      }
    };

    if rows.is_empty() {
      continue;
    }

    debug!("Marking rows as read");
    let placeholders: String = std::iter::repeat("?")
      .take(rows.len())
      .collect::<Vec<_>>()
      .join(",");
    loop {
      let update_read_query_sql = format!(
        "UPDATE {} SET was_read = 1 WHERE rowid IN ({});",
        &table_name, placeholders
      );
      let mut update_read_query = sqlx::query(&update_read_query_sql);
      for rowid in rows.keys() {
        update_read_query = update_read_query.bind(rowid);
      }
      if let Ok(_) = transaction.execute(update_read_query).await {
        break;
      }
    }
    if let Err(e) = transaction.commit().await {
      error!("Failed to commit transaction after reading data. {}", e);
    };
    drop(conn_lock); // TODO needed?
    yield_now().await;

    for row in rows.into_values() {
      match data_sender.send(row).await {
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
  db: Arc<Mutex<SqliteConnection>>,
  connection: (TcpStream, i64),
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
        let data = data.unwrap();
        debug!("Received from DB: {:?}", data);
        if let Err(e) = tcp.write_all(&data).await {
          error!("Failed to write to {}: {}", table_name, e);
        }
      },
      n = read_tcp => {
        let n = n.unwrap();
        if n == 0 {
          info!("Connection closed");
          break;
        }
        debug!("Read {} bytes data from tcp", n);
        let mut lock = db.lock().await;
        debug!("Writing data to DB table {}", &table_name);
        match SqliteConnection::begin(&mut *lock).await {
          Ok(mut transaction) => {
            match transaction.execute(sqlx::query(
              &format!("INSERT INTO {} (connection, data, was_read) VALUES ($1, $2, 0);", &table_name))
              .bind(connection_id)
              .bind(&tcp_buffer[0..n])).await {
              Ok(row) => row,
              Err(e) => {
                error!("Failed to insert data to DB: {}", e);
                continue;
              }
            };
            if let Err(e) = transaction.clear_cached_statements().await {
              error!("Failed to clear cache. {}", e);
            };
            if let Err(e) = transaction.commit().await {
              error!("Failed to commit transaction after inserting data. {}", e);
              continue;
            }
          },
          Err(e) => {
            error!("Failed to start transaction before inserting data. {}", e);
          }
        }
      }
    }
  }
}

async fn setup_host(
  address: String,
  connection_sender: Sender<(TcpStream, i64)>,
  db: Arc<Mutex<SqliteConnection>>,
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

async fn notify_new_host(db: Arc<Mutex<SqliteConnection>>) -> Result<i64, Error> {
  let mut lock = db.lock().await;
  let mut transaction = SqliteConnection::begin(&mut *lock).await?;
  let connection_id = transaction
    .fetch_one(sqlx::query(
      "INSERT INTO connections default values RETURNING connection;",
    ))
    .await
    .map(|row| row.get(0));
  transaction.commit().await?;
  connection_id
}

async fn setup_client(
  address: String,
  connection_sender: Sender<(TcpStream, i64)>,
  db: Arc<Mutex<SqliteConnection>>,
) -> anyhow::Result<()> {
  debug!("Starting client");
  loop {
    let connection_id = match poll_connection_id(db.clone()).await {
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

async fn poll_connection_id(db: Arc<Mutex<SqliteConnection>>) -> Option<i64> {
  match db.try_lock() {
    Ok(mut lock) => {
      match lock
        .fetch_one(sqlx::query(
          r#"
        SELECT connection FROM connections
        WHERE used = 0
        ORDER BY rowid
        LIMIT 1;"#,
        ))
        .await
        .map(|row| row.get(0))
      {
        Ok(connection) => Some(connection),
        Err(_e) => {
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
