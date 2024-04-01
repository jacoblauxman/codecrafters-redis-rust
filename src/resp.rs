use crate::Db;
use anyhow::Context;
use bytes::{Buf, BytesMut};
use std::time::{Duration, SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

const ECHO: &str = "ECHO";
const PING: &str = "PING";
const SET: &str = "SET";
const GET: &str = "GET";
const INFO: &str = "INFO";
const REPLCONF: &str = "REPLCONF";

#[derive(Debug)]
pub enum RespCommand {
    Ping,
    Echo(String),
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        val: Vec<u8>,
        expiry: Option<u64>,
    },
    Info(String),
    ReplConf,
    // ... more?
}

#[derive(Debug)]
pub enum RespFrame {
    Array(Vec<RespFrame>),
    BulkString(Vec<u8>),
    SimpleString(String),
    Error(String),
    NullBulkString,
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
            frame_bytes.extend(b"\r\n");
            // ^^note: needed for break between arr. len and content values -- TODO: clean up
            payload
                .iter()
                .for_each(|resp_frame| frame_bytes.extend(serialize(resp_frame)));

            frame_bytes
        }
        RespFrame::NullBulkString => format!("$-1\r\n").into_bytes(),
        RespFrame::Error(payload) => format!("-{}\r\n", payload).into_bytes(),
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    role: String,
    _master_replid: String,
    _master_repl_offset: u32,
    _master_host: Option<String>,
    _master_port: Option<String>,
    // todo: add more fields... ?
}

// #[derive(Debug)]
// pub enum ReplicationRole {
//     Master,
//     Slave
// }

impl ReplicationInfo {
    fn new_master() -> Self {
        Self {
            role: "master".to_string(),
            _master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            _master_repl_offset: 0,
            _master_host: None,
            _master_port: None,
        }
    }

    fn new_slave(master_host: String, master_port: String) -> Self {
        Self {
            role: "slave".to_string(),
            _master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            _master_repl_offset: 0,
            _master_host: Some(master_host),
            _master_port: Some(master_port),
        }
    }
}

pub struct RespHandler {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
    // todo: create role field for "info" + handling of `replicaof` cmd flag input value(s) -- new + default
    replication_info: ReplicationInfo,
}

impl RespHandler {
    // pub fn new(stream: TcpStream) -> Self {
    pub fn new(stream: TcpStream, replicaof_params: Option<(String, String)>) -> Self {
        let replication_info = if let Some((master_host, master_port)) = replicaof_params {
            ReplicationInfo::new_slave(master_host, master_port)
        } else {
            ReplicationInfo::new_master()
        };

        RespHandler {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(4 * 1024),
            replication_info,
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
            return Ok(Some(RespFrame::NullBulkString));
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
        // let data = std::str::from_utf8(&self.buf)?;
        // println!("DATA IN PARSE: {data:?}");
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
                                "Invalid RESP command format received while parsing `ECHO`"
                            ));
                        }
                    }
                    SET => {
                        if let Some(RespFrame::BulkString(key_bytes)) = resp_frames.get(1) {
                            if let Some(RespFrame::BulkString(val_bytes)) = resp_frames.get(2) {
                                let mut expiry = None;
                                if resp_frames.len() > 4 {
                                    if let Some(RespFrame::BulkString(flag_bytes)) =
                                        resp_frames.get(3)
                                    {
                                        let flag =
                                            String::from_utf8_lossy(&flag_bytes[..]).to_uppercase();
                                        if flag == "PX" {
                                            if let Some(RespFrame::BulkString(expiry_bytes)) =
                                                resp_frames.get(4)
                                            {
                                                expiry = Some(String::from_utf8_lossy(
                                                    &expiry_bytes[..],
                                                ).parse::<u64>().context("Invalid flag parameter provided while parsing `px` expiration time (ms)")?);
                                            }
                                        } else {
                                            return Err(anyhow::anyhow!("Invalid or Unimplemented RESP flag received while parsing `SET` command"));
                                        }
                                    }
                                }
                                return Ok(RespCommand::Set {
                                    key: key_bytes.clone(),
                                    val: val_bytes.clone(),
                                    expiry,
                                });
                            } else {
                                return Err(anyhow::anyhow!(
                                    "Invalid RESP format received while parsing `SET` command"
                                ));
                            }
                        } else {
                            return Err(anyhow::anyhow!(
                                "Invalid RESP format received while parsing `SET` command"
                            ));
                        }
                    }
                    GET => {
                        if let Some(RespFrame::BulkString(key_bytes)) = resp_frames.get(1) {
                            return Ok(RespCommand::Get {
                                key: key_bytes.clone(),
                            });
                        } else {
                            return Err(anyhow::anyhow!(
                                "Invalid Resp format received while parsing `GET` command"
                            ));
                        }
                    }
                    INFO => {
                        // todo: make dynamic + remove hardcoded "replication" value
                        return Ok(RespCommand::Info("replication".to_string()));
                    }
                    REPLCONF => {
                        return Ok(RespCommand::ReplConf);
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

    pub async fn write_frame(&mut self, resp_cmd: &RespCommand, db: &mut Db) -> anyhow::Result<()> {
        let resp_frame = match resp_cmd {
            RespCommand::Ping => RespFrame::SimpleString("PONG".to_string()),
            RespCommand::Echo(payload) => RespFrame::BulkString(payload.as_bytes().to_vec()),
            RespCommand::Get { key } => {
                let mut db = db.lock().unwrap();
                if let Some((val, expiry)) = db.get(key) {
                    if let Some(exp_time) = expiry {
                        if SystemTime::now() < *exp_time {
                            RespFrame::BulkString(val.clone())
                        } else {
                            db.remove(key);
                            RespFrame::NullBulkString
                        }
                    } else {
                        RespFrame::BulkString(val.clone())
                    }
                } else {
                    RespFrame::NullBulkString
                }
            }
            RespCommand::Set { key, val, expiry } => {
                let mut db = db.lock().unwrap();
                db.insert(
                    key.clone(),
                    (
                        val.clone(),
                        expiry.map(|ms| SystemTime::now() + Duration::from_millis(ms)),
                    ),
                );
                RespFrame::SimpleString("OK".to_string())
            }
            // todo: serialization method for `ReplicationInfo`?
            RespCommand::Info(_) => {
                match self.replication_info.role.to_uppercase().as_str() {
                    "MASTER" => {
                        let info = format!(
                            "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                            self.replication_info._master_replid,
                            self.replication_info._master_repl_offset
                        );
                        // RespFrame::BulkString("role:master".as_bytes().to_vec())
                        RespFrame::BulkString(info.as_bytes().to_vec())
                    }
                    "SLAVE" => RespFrame::BulkString("role:slave".as_bytes().to_vec()),
                    _ => unreachable!(),
                }
            }
            RespCommand::ReplConf => RespFrame::SimpleString("OK".to_string()),
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
            buf.advance(2);

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

// -- "set":
// *3\r\n$3\r\nset\r\n$10\r\nstrawberry\r\n$5\r\nmango\r\n
// -- response format:
// +OK\r\n

// -- "get":
// *2\r\n$3\r\nget\r\n$10\r\nstrawberry\r\n
// -- response format:
// $5\r\nmango\r\n

// -- "set" with "px":
// *5\r\n$3\r\nset\r\n$5\r\napple\r\n$9\r\npineapple\r\n$2\r\npx\r\n$3\r\n100\r\n
// -- response upon "get" after "px" (miliseconds) duration expired format:
// $-1\r\n
