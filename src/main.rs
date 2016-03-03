extern crate docopt;
extern crate kudu;
extern crate libc;
extern crate linenoise;
extern crate rustc_serialize;
extern crate term;

/// Returns the result of a parse if not successful, otherwise returns the value
/// and remaining input.
macro_rules! try_parse {
    ($e:expr) => (match $e {
        $crate::parser::ParseResult::Ok(t, remaining) => (t, remaining),
        $crate::parser::ParseResult::Incomplete(hints) =>
            return $crate::parser::ParseResult::Incomplete(hints),
        $crate::parser::ParseResult::Err(err, remaining) =>
            return $crate::parser::ParseResult::Err(err, remaining),
    });
}

mod command;
mod parser;
mod terminal;

use docopt::Docopt;

use parser::{
    Parser,
    ParseResult,
    Commands1,
};

static HELP: &'static str = "
Commands:

    SHOW TABLES;
        List the name of all Kudu tables.

    DESCRIBE TABLE <table>;
        Print a description of the table.
";

static USAGE: &'static str = "
Usage:
  kudusql [--master=<addr>]... [--color=<color>]

Options:
  -c --color=<color>        Whether to colorize output. Valid values are always,
                            never, or auto. [default: auto].
  -m --master=<addr>        Kudu master server address [default: 0.0.0.0:7051].
  -h --help                 Show a help message.
";

#[derive(Clone, Copy, Debug, RustcDecodable, PartialEq, Eq)]
pub enum Color {
    /// Colorize output unless the terminal is not a tty.
    Auto,

    /// Always colorize output.
    Always,

    /// Never colorize output.
    Never
}

#[derive(Debug, RustcDecodable)]
struct Args {
    flag_master: Vec<String>,
    flag_color: Color,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    let mut term = terminal::Terminal::new(args.flag_color);

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
                            command.execute(&mut client, &mut term);
                        }
                    },
                    ParseResult::Err(hints, remaining) => {
                        term.print_parse_error(input, remaining, &hints);
                    },
                    ParseResult::Incomplete(hints) => {
                        // Find the hint that made the most progress, and use it
                        // as the error.
                        let &(_, remaining) = hints.iter()
                                                   .min_by_key(|&&(_, remaining)| remaining.len())
                                                   .unwrap();
                        let hints = hints.into_iter()
                                         .filter(|&(_, ref r)| remaining.len() == r.len())
                                         .map(|t| t.0)
                                         .collect::<Vec<_>>();
                        term.print_parse_error(input, remaining, &hints);
                    },
                }
            },
        }
    }
}

fn callback(input: &str) -> Vec<String> {
    let mut completions = Vec::new();
    match parser::Command.parse(input) {
        parser::ParseResult::Incomplete(hints) => {
            for &(hint, remaining) in hints.iter() {
                let parsed = &input[..input.len() - remaining.len()];
                match hint {
                    parser::Hint::Constant(hint) => completions.push(format!("{}{}", parsed, hint)),
                    parser::Hint::Table(_) => (),
                }
            }
        },
        _ => (),
    }
    completions
}
