use anyhow::Context;
use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind TcpListener to address")?;

    loop {
        match listener.accept().await {
            Ok((mut stream, _socketaddr)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    if let Err(err) = connection_handler(&mut stream).await {
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

async fn connection_handler(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut res_buf = BytesMut::new();
    let mut req_buf = BytesMut::new();

    loop {
        req_buf.reserve(512);
        let req_read = stream
            .read_buf(&mut req_buf)
            .await
            .context("Failed to read in request from TcpStream")?;

        if req_read == 0 {
            break;
        }

        res_buf.put_slice(b"+PONG\r\n");
        stream
            .write_all(&res_buf)
            .await
            .context("Failed to write response buffer to TcpStream")?;

        stream
            .flush()
            .await
            .context("Failed to flush TcpStream response writer")?;
        res_buf.clear();
        req_buf.clear();
    }

    Ok(())
}
