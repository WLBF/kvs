use crate::common::*;
use crate::engines::*;
use crate::error::*;
use crate::thread_pool::ThreadPool;
use log::{debug, error};
use serde_json::Deserializer;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engines.
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer {
            engine,
            pool,
            handle: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run the server listening on the given address
    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        let shutdown = self.shutdown.clone();
        let engine = self.engine.clone();
        let pool = self.pool.clone();

        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let eng = engine.clone();
                        pool.spawn(|| {
                            if let Err(e) = serve(stream, eng) {
                                error!("error on serving client: {}", e);
                            }
                        });
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(e) => error!("encountered IO error: {}", e),
                }
            }
        });

        self.handle.replace(handle);
        Ok(())
    }

    /// Shutdown the server
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let handle = self.handle.take().unwrap();
        handle.join().unwrap();
    }
}

fn serve<E: KvsEngine>(tcp: TcpStream, engine: E) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    for req in req_reader {
        let req = req?;
        debug!("receive request from {}: {:?}", peer_addr, req);

        macro_rules! send_resp {
            ($resp:ident) => {
                debug!("send response to {}: {:?}", peer_addr, $resp);
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
