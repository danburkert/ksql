# ksql

An experimental SQL-like shell for Apache Kudu.

## Using `ksql`

Archives containing pre-compiled binaries are available for Linux and macOS on
the [releases](https://github.com/danburkert/ksql/releases) page. After starting
the shell, type `help;` to see available commands.

```bash
$ ksql
ksql> help;
```

## Building from Source

`ksql` is written in Rust, and uses Cargo, the Rust package manager for building
the project. The easiest way to install Rust and Cargo is through
[rustup](https://rustup.rs/). Once `cargo` is installed, building `ksql` is
easy:

```bash
# navigate to the checked-out ksql repository directory, then run ksql:
cargo run --release
```
