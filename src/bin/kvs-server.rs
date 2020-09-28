use clap::arg_enum;
use kvs::{self, thread_pool::*, KvStore, KvsServer, Result, SledKvsEngine};
use log::{error, info, LevelFilter};
use std::env::current_dir;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::Read;
use std::process::exit;
use std::thread;
use structopt::StructOpt;

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(StructOpt)]
#[structopt(author, about)]
struct Opt {
    /// Server ip address
    #[structopt(default_value = DEFAULT_ADDRESS, short, long)]
    addr: String,

    /// Engine name
    #[structopt(long, possible_values = & Engine::variants())]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}

fn main() {
    let opt = Opt::from_args();

    env_logger::builder().filter_level(LevelFilter::Info).init();

    if let Err(e) = start(opt) {
        error!("start failed: {}", e);
        exit(1);
    }
}

fn start(opt: Opt) -> Result<()> {
    let engine = get_engine(opt.engine)?;

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("storage engines: {}", engine);
    info!("listening on: {}", opt.addr);

    // write engines to engines file
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    let pool = RayonThreadPool::new(num_cpus::get() as u32)?;

    match engine {
        Engine::kvs => {
            let mut server = KvsServer::new(KvStore::open(current_dir()?)?, pool);
            server.run(opt.addr)?;
            loop {
                thread::park()
            }
        }
        Engine::sled => {
            let mut server = KvsServer::new(SledKvsEngine::new(sled::open(current_dir()?)?), pool);
            server.run(opt.addr)?;
            loop {
                thread::park()
            }
        }
    }
}

fn get_engine(arg: Option<Engine>) -> Result<Engine> {
    let path = current_dir()?.join("engine");
    let cur = if path.exists() {
        let mut f = File::open(path)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        Some(s.parse().expect("invalid engines string"))
    } else {
        None
    };

    match (arg, cur) {
        (None, None) => Ok(DEFAULT_ENGINE),
        (None, Some(ce)) => Ok(ce),
        (Some(ae), None) => Ok(ae),
        (Some(ae), Some(ce)) => {
            if ae != ce {
                error!("engines not match");
                exit(1);
            }
            Ok(ae)
        }
    }
}
