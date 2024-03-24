use anyhow::Context;
use redis_starter_rust::resp::RespHandler;
use redis_starter_rust::Db;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")
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
