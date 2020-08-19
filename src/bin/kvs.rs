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
    match Opt::from_args() {
        Opt::Set { key, value } => {
            println!("set {} {}", key, value);
            eprintln!("unimplemented");
            exit(1)
        }
        Opt::Get { key } => {
            println!("get {}", key);
            eprintln!("unimplemented");
            exit(1)
        }
        Opt::Rm { key } => {
            println!("rm {}", key);
            eprintln!("unimplemented");
            exit(1)
        }
    }
}
