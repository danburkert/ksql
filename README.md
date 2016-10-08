# ksql

An experimental SQL-like shell for Apache Kudu.

# Building from Source

`ksql` is written in Rust, and uses `Cargo`, the Rust package manager for
building the project. The easiest way to install Rust and Cargo is through
[rustup](https://rustup.rs/). Once `cargo` is installed, building `ksql` is
easy:

```bash
# navigate to the checked-out ksql repository directory, then
# build ksql:
cargo build --release

# run the ksql binary:
target/release/ksql
```
