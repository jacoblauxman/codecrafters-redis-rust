use anyhow::Context;
use redis_starter_rust::resp::RespHandler;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind TCP Listener to address")?;

    loop {
        match listener.accept().await {
            Ok((stream, _socketaddr)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    if let Err(err) = connection_handler(stream).await {
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

async fn connection_handler(stream: TcpStream) -> anyhow::Result<()> {
    let mut resp_handler = RespHandler::new(stream);

    loop {
        if let Some(req_frame) = resp_handler.read_frame().await? {
            let resp_cmd = resp_handler.parse_command(&req_frame)?;
            let _res_frame = resp_handler.write_frame(&resp_cmd).await?;
        }
    }
}
