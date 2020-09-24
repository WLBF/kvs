use crate::common::*;
use crate::engines::*;
use crate::error::*;
use crate::thread_pool::ThreadPool;
use log::{debug, error};
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engines.
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    /// Run the server listening on the given address
    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    self.pool.spawn(|| {
                        if let Err(e) = serve(stream, engine) {
                            error!("Error on serving client: {}", e);
                        }
                    });
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(tcp: TcpStream, engine: E) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    for req in req_reader {
        let req = req?;
        debug!("Receive request from {}: {:?}", peer_addr, req);

        macro_rules! send_resp {
            ($resp:ident) => {
                debug!("Send response to {}: {:?}", peer_addr, $resp);
                serde_json::to_writer(&mut writer, &$resp)?;
                writer.flush()?;
            };
        }

        match req {
            Request::Get { key } => {
                let resp = match engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                };
                send_resp!(resp);
            }
            Request::Set { key, value } => {
                let resp = match engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                };
                send_resp!(resp);
            }
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                };
                send_resp!(resp);
            }
        };
    }

    Ok(())
}
