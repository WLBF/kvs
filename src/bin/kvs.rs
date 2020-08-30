use kvs::{KvStore, KvsError};
use std::env::current_dir;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(author, about)]
enum Opt {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() {
    let cur_dir = current_dir().expect("unable to get current dir");
    let mut kv = KvStore::open(cur_dir).unwrap_or_else(|e| {
        eprintln!("{:?}", e);
        exit(1);
    });

    match Opt::from_args() {
        Opt::Set { key, value } => {
            if let Err(e) = kv.set(key, value) {
                eprintln!("{:?}", e);
                exit(1);
            }
        }
        Opt::Get { key } => match kv.get(key) {
            Ok(Some(value)) => println!("{}", value),
            Ok(None) => println!("Key not found"),
            Err(e) => {
                eprintln!("{:?}", e);
                exit(1);
            }
        },
        Opt::Rm { key } => {
            if let Err(e) = kv.remove(key) {
                match e {
                    KvsError::KeyNotFound => println!("Key not found"),
                    _ => eprintln!("{:?}", e),
                }
                exit(1);
            }
        }
    }
}
