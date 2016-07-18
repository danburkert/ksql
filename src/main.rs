extern crate chrono;
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

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;

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

    SHOW CREATE TABLE <table>;
        Prints the CREATE TABLE statement for the table.

    SHOW MASTERS;
        List the master servers in the Kudu cluster.

    SHOW TABLET SERVERS;
        List the tablet servers in the Kudu cluster.

    SHOW TABLETS OF TABLE <table>;
        List the tablets belonging to a table.

    SHOW TABLET REPLICAS OF TABLE <table>;
        List the tablet replicas belonging to a table.

    DESCRIBE TABLE <table>;
        List the columns of a table.

    DROP TABLE <table>;
        Delete the table.

    CREATE TABLE <table> (<col> <data-type> [NULLABLE | NOT NULL]
                                            [ENCODING <encoding>]
                                            [COMPRESSION <compression>]
                                            [BLOCK SIZE <block-size>], ..)
    PRIMARY KEY (<col>, ..)
    DISTRIBUTE BY [RANGE (<col>, ..) [SPLIT ROWS (<col-val>, ..)[, (<col-val>, ..)..]]]
                  [HASH (<col>, ..) [WITH SEED <seed>] INTO <buckets> BUCKETS]..
    WITH <replicas> REPLICAS;
        Create a table with the specified columns and options.
";

/*
    INSERT INTO <table> [(<col>, ..)] VALUES (<col-val>, ..), ..;
        Insert one or more rows into the table. The column order may optionally
        be specified.

    SELECT * FROM <table>;
    SELECT <col>,.. FROM <table>;
        Select all or some columns from a table.

    SELECT COUNT(*) FROM <table>;
        Count the total number of rows in the table.
*/

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

    let client = {
        let master_addrs = args.flag_master.iter().map(|master| resolve_master(master)).collect();
        let config = kudu::ClientConfig::new(master_addrs);
        kudu::Client::new(config)
    };

    linenoise::set_callback(callback);
    linenoise::set_multiline(1);

    let mut input: Option<String> = None;
    loop {
        match input {
            Some(ref mut input) => match linenoise::input("") {
                Some(ref line) => {
                    input.push(' ');
                    input.push_str(&line)
                },
                None => break,
            },
            None => input = linenoise::input("kudu> "),
        }
        match input {
            None => { break }
            Some(ref input) => {
                match Commands1.parse(&input) {
                    ParseResult::Ok(commands, remaining) => {
                        linenoise::history_add(&input);
                        assert!(remaining.is_empty());
                        for command in commands {
                            command.execute(&client, &mut term);
                        }
                    },
                    ParseResult::Err(hints, remaining) => {
                        linenoise::history_add(&input);
                        term.print_parse_error(&input, remaining, &hints);
                    },
                    _ => continue,
                }
            },
        }
        input = None;
    }
}

/// Attempts to resolve a string into a master address. Panic on failure.
fn resolve_master(input: &str) -> SocketAddr {
    if let Ok(addr) = SocketAddr::from_str(input) {
        return addr;
    }
    if let Ok(ip) = IpAddr::from_str(input) {
        return SocketAddr::new(ip, 7051);
    }
    if let Ok(mut results) = input.to_socket_addrs() {
        if let Some(addr) = results.next() {
            return addr;
        }
    }
    if let Ok(mut results) = (input, 7051).to_socket_addrs() {
        if let Some(addr) = results.next() {
            return addr;
        }
    }

    panic!("Unable to resolve master address '{}'", input);
}

fn callback(input: &str) -> Vec<String> {
    let mut completions = Vec::new();
    match parser::Command.parse(input) {
        parser::ParseResult::Incomplete(hints) => {
            for &(hint, remaining) in hints.iter() {
                let parsed = &input[..input.len() - remaining.len()];
                match hint {
                    parser::Hint::Constant(hint) => completions.push(format!("{}{}", parsed, hint)),
                    _ => (),
                }
            }
        },
        _ => (),
    }
    completions
}
