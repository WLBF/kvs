use kvs::*;
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

#[derive(StructOpt)]
#[structopt(author, about)]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Set the value of a string key to a string
    Set {
        key: String,
        value: String,

        /// Server ip address
        #[structopt(default_value = DEFAULT_ADDRESS, short, long)]
        addr: String,
    },

    /// Get the string value of a given string key
    Get {
        key: String,

        /// Server ip address
        #[structopt(default_value = DEFAULT_ADDRESS, short, long)]
        addr: String,
    },

    /// Remove a given string key
    Rm {
        key: String,

        /// Server ip address
        #[structopt(default_value = DEFAULT_ADDRESS, short, long)]
        addr: String,
    },
}

fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Rm { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}
