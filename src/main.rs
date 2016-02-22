extern crate docopt;
extern crate kudu;
extern crate linenoise;
extern crate rustc_serialize;

/// Returns the result of a parse if not successful, otherwise returns the value
/// and remaining input.
macro_rules! try_parse {
    ($e:expr) => (match $e {
        $crate::parser::ParseResult::Ok(t, remaining) => (t, remaining),
        $crate::parser::ParseResult::Incomplete(hint, remaining) =>
            return $crate::parser::ParseResult::Incomplete(hint, remaining),
        $crate::parser::ParseResult::Err(err, remaining) =>
            return $crate::parser::ParseResult::Err(err, remaining),
    });
}

mod command;
mod format;
mod parser;

use std::io;

use docopt::Docopt;

use parser::{
    Parser,
    ParseResult,
    Hint,
    Commands1,
};

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

    let mut client = {
        let mut builder = kudu::ClientBuilder::new();
        for addr in &args.flag_master {
            builder.add_master_server_addr(addr);
        }
        builder.build().unwrap()
    };

  linenoise::set_callback(callback);
  linenoise::set_multiline(1);

    loop {
      let val = linenoise::input("kudu> ");
        match val {
            None => { break }
            Some(ref input) => {
                match Commands1.parse(input) {
                    ParseResult::Ok(commands, remaining) => {
                        assert!(remaining.is_empty());
                        for command in commands {
                            command.execute(&mut client);
                        }
                    },
                    ParseResult::Err(expected, remaining) => {
                        let stderr = io::stderr();
                        format::write_parse_error(&mut stderr.lock(), input, remaining, expected);
                    },
                    _ => (),
                }
            },
        }
    }
}

fn callback(input: &str) -> Vec<String> {
    match parser::Command.parse(input) {
        parser::ParseResult::Incomplete(hint, remaining) => {
            let parsed = &input[..input.len() - remaining.len()];
            match hint {
                parser::Hint::Constant(hint) => vec![format!("{}{}", parsed, hint)],
                parser::Hint::Table(_) => vec![],
            }
        },
        _ => vec![]
    }
}
