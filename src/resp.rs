use anyhow::Context;
use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

const ECHO: &str = "ECHO";
const PING: &str = "PING";

#[derive(Debug)]
pub enum RespCommand {
    Ping,
    Echo(String),
    // Get {
    //     key: String,
    // },
    // Set {
    //     key: String,
    //     val: Bytes,
    // },
    // ... more?
}

#[derive(Debug)]
pub enum RespFrame {
    Array(Vec<RespFrame>),
    BulkString(Vec<u8>),
    SimpleString(String),
    Error(String),
    Null,
}

pub fn serialize(resp_frame: &RespFrame) -> Vec<u8> {
    match resp_frame {
        RespFrame::SimpleString(payload) => format!("+{}\r\n", payload).into_bytes(),
        RespFrame::BulkString(payload) => {
            let mut bulk_bytes = Vec::new();
            let bulk_len = payload.len().to_string();

            bulk_bytes.push(b'$');
            bulk_bytes.extend(bulk_len.into_bytes());
            bulk_bytes.extend(b"\r\n");
            bulk_bytes.extend(payload.iter());
            bulk_bytes.extend(b"\r\n");

            bulk_bytes
        }
        RespFrame::Array(payload) => {
            let mut frame_bytes = Vec::new();

            frame_bytes.push(b'*');
            frame_bytes.extend(payload.len().to_string().into_bytes());
            payload
                .iter()
                .for_each(|resp_frame| frame_bytes.extend(serialize(resp_frame)));

            frame_bytes
        }
        RespFrame::Null => format!("$-1\r\n").into_bytes(),
        RespFrame::Error(payload) => format!("-{}\r\n", payload).into_bytes(),
    }
}

pub struct RespHandler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(4 * 1024),
        }
    }

    // -- READ / REQUEST -- //

    fn parse_simple_string(&mut self) -> anyhow::Result<Option<RespFrame>> {
        self.buf.advance(1);
        let payload = read_crlf_line(&mut self.buf)?.unwrap();
        let simple_string = String::from_utf8(payload)
            .map(RespFrame::SimpleString)
            .context("Invalid UTF-8 data present while parsing Simple String")?;
        Ok(Some(simple_string))
    }

    fn parse_bulk_string(&mut self) -> anyhow::Result<Option<RespFrame>> {
        self.buf.advance(1);
        let bulk_bytes_line = read_crlf_line(&mut self.buf)
            .context("Error: Failed to parse valid array length from received payload")?
            .unwrap();
        let frame_len = String::from_utf8(bulk_bytes_line)?.parse::<usize>()?;

        if frame_len as i64 == -1 {
            self.buf.advance(2);
            return Ok(Some(RespFrame::Null));
        }

        let payload = read_crlf_line(&mut self.buf)?.unwrap();
        return Ok(Some(RespFrame::BulkString(payload)));
    }

    fn parse_array(&mut self) -> anyhow::Result<Option<RespFrame>> {
        self.buf.advance(1);

        let num_frames_line = read_crlf_line(&mut self.buf)
            .context("Error: Failed to parse valid array length from received payload")?
            .unwrap();
        let frame_len = String::from_utf8(num_frames_line)?.parse::<usize>()?;
        let mut frame_arr = Vec::with_capacity(frame_len as usize);

        for _ in 0..frame_len {
            if let Some(frame) = self.parse_frame()? {
                frame_arr.push(frame);
            } else {
                return Err(anyhow::anyhow!(
                    "Incorrectly formatted RESP payload received while parsing frames in array frame data"
                ));
            }
        }

        Ok(Some(RespFrame::Array(frame_arr)))
    }

    pub fn parse_frame(&mut self) -> anyhow::Result<Option<RespFrame>> {
        if !self.buf.has_remaining() {
            return Ok(None);
        }

        match self.buf[0] {
            b'+' => self.parse_simple_string(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => Err(anyhow::anyhow!(
                "Invalid or Unimplemented frame data type byte"
            )),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<RespFrame>> {
        loop {
            if let Some(resp_frame) = self.parse_frame()? {
                return Ok(Some(resp_frame));
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow::anyhow!("Connection reset by client"));
                }
            }
        }
    }

    // -- COMMAND / ACTION -- //

    pub fn parse_command(&mut self, resp_frame: &RespFrame) -> anyhow::Result<RespCommand> {
        if let RespFrame::Array(resp_frames) = resp_frame {
            if let Some(RespFrame::BulkString(cmd_bytes)) = resp_frames.get(0) {
                let cmd = String::from_utf8_lossy(&cmd_bytes[..]);
                match cmd.to_uppercase().as_str() {
                    PING => Ok(RespCommand::Ping),
                    ECHO => {
                        if let Some(RespFrame::BulkString(echo_bytes)) = resp_frames.get(1) {
                            let echo_string = String::from_utf8_lossy(&echo_bytes[..]).to_string();

                            return Ok(RespCommand::Echo(echo_string));
                        } else {
                            return Err(anyhow::anyhow!(
                                "Invalid RESP command format received while parsing"
                            ));
                        }
                    }
                    _ => todo!(), // other commands to be implemented
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Protocol Error: Invalid RESP command format received within array frame (must received bulk string data type)"
                ));
            }
        } else {
            return Err(anyhow::anyhow!(
                "Protocol Error: Invalid RESP command format received (must receive array data type)"
            ));
        }
    }

    // -- WRITE / RESPONSE -- //

    pub async fn write_frame(&mut self, resp_cmd: &RespCommand) -> anyhow::Result<()> {
        let resp_frame = match resp_cmd {
            RespCommand::Ping => RespFrame::SimpleString("PONG".to_string()),
            RespCommand::Echo(payload) => RespFrame::BulkString(payload.as_bytes().to_vec()),
        };
        let frame_bytes = serialize(&resp_frame);

        self.stream
            .write_all(&frame_bytes)
            .await
            .context("Failed to write respone frame to TCP stream")?;

        self.stream
            .flush()
            .await
            .context("Failed to flush TCP stream after write")?;

        self.buf.clear();

        Ok(())
    }
}

pub fn read_crlf_line(buf: &mut BytesMut) -> anyhow::Result<Option<Vec<u8>>> {
    for i in 1..buf.len() {
        if buf[i - 1] == b'\r' && buf[i] == b'\n' {
            let line = buf.split_to(i - 1).to_vec();
            buf.advance(2); // NOTE: may need to be adv. '1' instead?

            return Ok(Some(line));
        }
    }
    Err(anyhow::anyhow!(
        "Fromatting error in reading received payload -- failed to find CRLF delimeter in request"
    ))
}

// NOTES on data shape / cmds:

// -- "ping":
// *1\r\n$4\r\nping\r\n
// -- response format:
// +PONG\r\n

// -- "echo":
// *2\r\n$4\r\necho\r\n$3\r\nhey\r\n
// -- response format:
// $3\r\nhey\r\n