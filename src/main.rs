extern crate docopt;
extern crate kudu;
extern crate linenoise;
extern crate rustc_serialize;

#[macro_use]
extern crate nom;

mod command;
mod parser;

use docopt::Docopt;

static USAGE: &'static str = "
Usage:
  kudusql [--master=<addr>]...

Options:
  -m --master=<addr>        Kudu master server address [default: 0.0.0.0:7051].
  -h --help                 Show a help message.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    flag_master: Vec<String>,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    let client = {
        let mut builder = kudu::ClientBuilder::new();
        for addr in &args.flag_master {
            builder.add_master_server_addr(addr);
        }
        builder.build().unwrap()
    };

  linenoise::set_callback(callback);

    loop {
      let val = linenoise::input("kudu> ");
        match val {
            None => { break }
            Some(ref input) if &*input == "show tables;" => {
                match client.list_tables() {
                    Ok(tables) => {
                        for table in tables {
                            println!("{}", table);
                        }
                    }
                    Err(error) => {
                        println!("{}", error);
                    }
                }
            }
            Some(_) => {
                println!("unknown command");
            }
        }
    }
}

fn callback(input: &str) -> Vec<String> {
    let ret = if input.starts_with("s") {
        vec!["show tables;"]
    } else {
        vec![]
    };

    return ret.iter().map(|s| s.to_string()).collect();
}
