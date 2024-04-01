use anyhow::Context;
use redis_starter_rust::resp::{serialize, RespFrame, RespHandler};
use redis_starter_rust::Db;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = parse_args(std::env::args().skip(1));

    let port = if let Some(port_val) = flags.get("port") {
        port_val[0].clone()
    } else {
        "6379".to_string()
    };
    port.parse::<u16>().context("Invalid port value provided")?; // validation check

    let replicaof_args = if let Some(replicaof_val) = flags.get("replicaof") {
        if replicaof_val.len() >= 2 {
            Some((replicaof_val[0].clone(), replicaof_val[1].clone()))
        } else {
            None
        }
    } else {
        None
    };

    if replicaof_args.is_some() {
        let (master_host, master_port) = replicaof_args.clone().unwrap();
        send_connection_handshake(master_host, master_port, &port).await?;
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .context("Failed to bind TCP Listener to address")?;

    let resp_db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _socketaddr)) => {
                println!("accepted new connection");
                let mut db = resp_db.clone();
                let replicaof_params = replicaof_args.clone();
                tokio::spawn(async move {
                    if let Err(err) = connection_handler(stream, &mut db, replicaof_params).await {
                        eprintln!("Error handling connection: {:?}", err);
                    }
                });
            }
            Err(err) => {
                println!("ERROR: {}", err);
            }
        };
    }
}

// async fn connection_handler(stream: TcpStream, resp_db: &mut Db) -> anyhow::Result<()> {
async fn connection_handler(
    stream: TcpStream,
    resp_db: &mut Db,
    replicaof_params: Option<(String, String)>,
) -> anyhow::Result<()> {
    let mut resp_handler = RespHandler::new(stream, replicaof_params.clone());

    loop {
        if let Some(req_frame) = resp_handler.read_frame().await? {
            let resp_cmd = resp_handler.parse_command(&req_frame)?;
            let _ = resp_handler.write_frame(&resp_cmd, resp_db).await?;
        }
    }
}

pub async fn send_connection_handshake(
    mut master_host: String,
    master_port: String,
    port: &str,
) -> anyhow::Result<()> {
    if master_host.as_str() == "localhost" {
        master_host = "127.0.0.1".to_string();
    }
    let master_addr = format!("{}:{}", master_host, master_port);

    // 1) PING
    let ping_frame = RespFrame::Array(vec![RespFrame::BulkString("PING".as_bytes().to_vec())]);
    let ping_bytes = serialize(&ping_frame);

    let mut master_stream = TcpStream::connect(&master_addr)
        .await
        .context("Failed to connect to `master` instance")?;

    master_stream
        .write_all(&ping_bytes)
        .await
        .context("Failed to send PING command to `master` instance")?;

    master_stream
        .flush()
        .await
        .context("Failed to flush handshake stream after PING command sent to `master` instance")?;

    let mut master_res_buf = [0; 512];
    master_stream
        .read(&mut master_res_buf)
        .await
        .context("Failed to read or receive `pong` response from `master` instance in handshake")?;

    // 2) REPLCONF

    // `Listening`
    let replconf_listen_bytes = serialize(&RespFrame::Array(vec![
        RespFrame::BulkString(b"REPLCONF".to_vec()),
        RespFrame::BulkString(b"listening-port".to_vec()),
        // RespFrame::BulkString(b"{port}".to_vec()),
        // RespFrame::BulkString(port.to_be_bytes().to_vec()),
        RespFrame::BulkString(port.as_bytes().to_vec()),
    ]));

    master_stream
        .write_all(&replconf_listen_bytes)
        .await
        .context("Failed to write `REPLCONF` for handshake to `master` instance")?;

    master_stream.flush().await.context(
        "Failed to flush stream to `master` after writing `REPLCONF` 'listening' command",
    )?;

    master_stream
        .read(&mut master_res_buf)
        .await
        .context("Failed to read or receive `OK` response from `master` instance after `REPLCONF` 'listening' cmd in handshake")?;

    // `Capabilities`
    let replconf_capa_bytes = serialize(&RespFrame::Array(vec![
        RespFrame::BulkString(b"REPLCONF".to_vec()),
        RespFrame::BulkString(b"capa".to_vec()),
        RespFrame::BulkString(b"psync2".to_vec()),
    ]));

    master_stream
        .write_all(&replconf_capa_bytes)
        .await
        .context("Failed to write `REPLCONF` for handshake to `master` instance")?;

    master_stream.flush().await.context(
        "Failed to flush stream to `master` after writing `REPLCONF` 'capabilities' command",
    )?;

    master_stream
        .read(&mut master_res_buf)
        .await
        .context("Failed to read or receive `OK` response from `master` instance after `REPLCONF` 'capabilities' cmd in handshake")?;

    // 3) PSYNC

    let psync_init_bytes = serialize(&RespFrame::Array(vec![
        RespFrame::BulkString("PSYNC".as_bytes().to_vec()),
        RespFrame::BulkString("?".as_bytes().to_vec()),
        RespFrame::BulkString("-1".as_bytes().to_vec()),
    ]));

    master_stream
        .write_all(&psync_init_bytes)
        .await
        .context("Failed to write `PSYNC` cmd for handshake to `master` instance")?;

    master_stream
        .flush()
        .await
        .context("Failed to flush stream to `master` after writing `PSYNC` command")?;

    master_stream
        .read(&mut master_res_buf)
        .await
        .context("Failed to read or receive `FULLRESYNC` response from `master` instance after `PYSNC` cmd in handshake")?;

    Ok(())
}

//
//
//

fn parse_args(args: impl Iterator<Item = String>) -> HashMap<String, Vec<String>> {
    let mut flags = HashMap::new();
    let mut iter = args.peekable();

    while let Some(mut arg) = iter.next() {
        if arg.starts_with("--") {
            let flag = arg.split_off(2);
            let mut flag_values = Vec::new();

            while let Some(peeked_arg) = iter.peek() {
                if peeked_arg.starts_with("--") {
                    break;
                } else {
                    flag_values.push(iter.next().unwrap()); // `next` = pushed peeked arg is a value associated to prior flag
                }
            }
            flags.insert(flag, flag_values);
        }
    }

    flags
}
