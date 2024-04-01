use anyhow::Context;
use redis_starter_rust::resp::RespHandler;
use redis_starter_rust::Db;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = parse_args(std::env::args().skip(1));
    // println!("FLAGS: {:?}", flags);

    let port = if let Some(port_val) = flags.get("port") {
        port_val[0].clone()
    } else {
        "6379".to_string()
    }
    .parse::<u16>()
    .context("Invalid port value provided")?;

    let _replicaof_params = if let Some(replicaof_val) = flags.get("replicaof") {
        if replicaof_val.len() >= 2 {
            Some((replicaof_val[0].clone(), replicaof_val[1].clone()))
        } else {
            None
        }
    } else {
        None
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .context("Failed to bind TCP Listener to address")?;

    let resp_db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _socketaddr)) => {
                println!("accepted new connection");
                let mut db = resp_db.clone();
                tokio::spawn(async move {
                    if let Err(err) = connection_handler(stream, &mut db).await {
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

async fn connection_handler(stream: TcpStream, resp_db: &mut Db) -> anyhow::Result<()> {
    let mut resp_handler = RespHandler::new(stream);

    loop {
        if let Some(req_frame) = resp_handler.read_frame().await? {
            let resp_cmd = resp_handler.parse_command(&req_frame)?;
            let _ = resp_handler.write_frame(&resp_cmd, resp_db).await?;
        }
    }
}

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
