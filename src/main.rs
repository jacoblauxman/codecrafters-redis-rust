use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                connection_handler(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn connection_handler(stream: &mut TcpStream) {
    let mut req_buf = [0; 128];
    let _req_read = stream.read(&mut req_buf);

    stream
        .write("+PONG\r\n".as_bytes())
        .expect("Faield to write PONG response to stream");
}
