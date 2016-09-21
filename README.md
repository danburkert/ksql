# kudusql

An experimental shell for Apache Kudu. Limited functionality.

# Building

`kudusql` is written in Rust, and uses `Cargo`, the Rust package manager for
building the project. The easiest way to install Rust and Cargo is through
[rustup](https://rustup.rs/). Once `cargo` is installed, building `kudusql` is
easy:

```bash
# navigate to the checked-out kudusql repository directory, then
# build kudusql:
cargo build --release

# run the kudusql binary:
target/release/kudusql
```
