use std::fmt;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono;
use futures::{Future, Stream};
use itertools::Itertools;
use kudu;
use libc;
use prettytable;
use term;

use parser::Hint;
use Color;
use HELP;

pub struct Terminal {
    out: Box<term::StdoutTerminal>,
    err: Box<term::StderrTerminal>,
    color: bool,
}

const HEX_CHARS: &'static [u8] = b"0123456789abcdef";
fn to_hex(bytes: &[u8]) -> String {
    let mut v = Vec::with_capacity(bytes.len() * 2 + 2);
    v.push('0' as u8);
    v.push('x' as u8);
    for &byte in bytes {
        v.push(HEX_CHARS[(byte >> 4) as usize]);
        v.push(HEX_CHARS[(byte & 0xf) as usize]);
    }

    unsafe { String::from_utf8_unchecked(v) }
}
fn to_date_time(timestamp: &SystemTime) -> chrono::DateTime<chrono::Utc> {
    let naive = match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => chrono::NaiveDateTime::from_timestamp(
            duration.as_secs() as i64,
            duration.subsec_nanos(),
        ),
        Err(err) => {
            let duration = err.duration();
            if duration.subsec_nanos() > 0 {
                chrono::NaiveDateTime::from_timestamp(
                    (-(duration.as_secs() as i64)) - 1,
                    1_000_000_000 - duration.subsec_nanos(),
                )
            } else {
                chrono::NaiveDateTime::from_timestamp(-(duration.as_secs() as i64), 0)
            }
        }
    };
    chrono::DateTime::from_utc(naive, chrono::Utc)
}

// TODO: eventually this shouldn't need to return an error.
pub fn row_to_cell(row: &kudu::Row) -> Result<prettytable::row::Row, kudu::Error> {
    let cells = row
        .schema()
        .columns()
        .iter()
        .enumerate()
        .map(move |(idx, column)| {
            let cell = if row.is_null(idx)? {
                cell!("null")
            } else {
                match column.data_type() {
                    kudu::DataType::Bool => cell!(row.get::<_, bool>(idx)?),
                    kudu::DataType::Int8 => cell!(row.get::<_, i8>(idx)?),
                    kudu::DataType::Int16 => cell!(row.get::<_, i16>(idx)?),
                    kudu::DataType::Int32 => cell!(row.get::<_, i32>(idx)?),
                    kudu::DataType::Int64 => cell!(row.get::<_, i64>(idx)?),
                    kudu::DataType::Float => cell!(row.get::<_, f32>(idx)?),
                    kudu::DataType::Double => cell!(row.get::<_, f64>(idx)?),
                    kudu::DataType::Timestamp => {
                        cell!(to_date_time(&row.get::<_, SystemTime>(idx)?))
                    }
                    kudu::DataType::String => cell!(format!("{:?}", row.get::<_, &str>(idx)?)),
                    kudu::DataType::Binary => cell!(to_hex(try!(row.get::<_, &[u8]>(idx)))),
                }
            };
            Ok(cell)
        })
        .collect::<Result<Vec<prettytable::cell::Cell>, kudu::Error>>()?;
    Ok(prettytable::row::Row::new(cells))
}

impl Terminal {
    /// Creates a new terminal.
    pub fn new(color: Color) -> Terminal {
        let color = match color {
            Color::Auto => unsafe { libc::isatty(libc::STDERR_FILENO) != 0 },
            Color::Always => true,
            Color::Never => false,
        };

        Terminal {
            out: term::stdout().expect("unable to open stdout"),
            err: term::stderr().expect("unable to open stderr"),
            color: color,
        }
    }

    /// Colors stderr red.
    fn red(&mut self) {
        if self.color {
            self.err.fg(term::color::BRIGHT_RED).unwrap();
        }
    }

    /// Style output to stderr as bold.
    fn bold(&mut self) {
        if self.color {
            self.err.attr(term::Attr::Bold).unwrap();
        }
    }

    /// Resets the style and color of stderr to the default.
    fn reset(&mut self) {
        if self.color {
            self.err.reset().unwrap();
        }
    }

    /// Prints 'msg' to stderr, in red.
    fn print_stderr_red(&mut self, msg: &str) {
        self.red();
        write!(self.err, "{}", msg).unwrap();
        self.reset();
    }

    /// Prints 'msg' to stderr, in bold.
    fn print_stderr_bold<D>(&mut self, msg: &D) where D: fmt::Display {
        self.bold();
        write!(self.err, "{}", msg).unwrap();
        self.reset();
    }

    /// Prints a blank line to stdout, for separation.
    fn print_newline(&mut self) {
        writeln!(self.out, "").unwrap();
    }

    /// Prints a parse error to stderr.
    pub fn print_parse_error(&mut self, input: &str, remaining: &str, hints: &[Hint]) {
        let error_idx = input.len() - remaining.len();
        assert_eq!(&input[error_idx..], remaining);
        let mut line_end_idx = 0;
        for line in input.lines() {
            line_end_idx += 1 + line.len();
            if line_end_idx >= error_idx {
                let hints = hints
                    .into_iter()
                    .map(|hint| match hint {
                        &Hint::Constant(ref expected) => format!("'{}'", *expected,),
                        &Hint::Integer => "an integer".to_string(),
                        &Hint::PosInteger => "a positive integer".to_string(),
                        &Hint::Float => "a floating point value".to_string(),
                        &Hint::Timestamp => "a timestamp value".to_string(),
                        &Hint::CharEscape => "a character escape sequence".to_string(),
                        &Hint::HexEscape => "a hex escape sequence".to_string(),
                        &Hint::Table(_) => "a table name".to_string(),
                        &Hint::Column(_) => "a column name".to_string(),
                    })
                    .collect::<Vec<_>>();
                self.print_error(&format!("expected: {}", &hints.join(" or ")));

                let word_len = input[error_idx..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .len();
                writeln!(self.err, "{}", line).unwrap();
                let arrow_idx = line.len() + 1 + error_idx - line_end_idx;
                writeln!(self.err, "{:>2$}{:~<3$}", "", "^", arrow_idx, word_len).unwrap();
                break;
            }
        }
    }

    /// Prints an error message to stderr.
    pub fn print_error<D>(&mut self, msg: &D) where D: fmt::Display {
        self.print_stderr_red("error: ");
        self.print_stderr_bold(msg);
        self.print_newline();
    }

    /// Prints a message to stdout.
    pub fn print_success<D>(&mut self, msg: &D) where D: fmt::Display {
        writeln!(self.out, "{}", msg).unwrap();
    }

    /// Prints a warning message to stderr.
    pub fn print_warning<D>(&mut self, msg: &D) where D: fmt::Display {
        self.print_stderr_red("warning: ");
        self.print_stderr_bold(msg);
        self.print_newline();
    }

    /// Prints the help message to stdout.
    pub fn print_help(&mut self) {
        self.print_success(&HELP);
    }

    /// Prints the master servers.
    pub fn print_masters(&mut self, masters: Vec<kudu::MasterInfo>) {
        let mut table = table![[
            "ID",
            "RPC Addresses",
            "HTTP Addresses",
            "Sequence Number",
            "Role"
        ]];
        for master in masters {
            table.add_row(row![
                master.id(),
                master.rpc_addrs().iter().join(", "),
                master.http_addrs().iter().join(", "),
                master.seqno().to_string(),
                format!("{:?}", master.role())
            ]);
        }
        self.print_prettytable(table);
    }

    /// Prints the tablet servers.
    pub fn print_tablet_servers(&mut self, tablet_servers: Vec<kudu::TabletServerInfo>) {
        let mut table = table![[
            "Tablet Server ID",
            "RPC Addresses",
            "HTTP Addresses",
            "Software Version",
            "Sequence Number",
            "Last Heartbeat"
        ]];
        for tablet_server in tablet_servers {
            table.add_row(row![
                tablet_server.id(),
                tablet_server.rpc_addrs().iter().join(", "),
                tablet_server.http_addrs().iter().join(", "),
                tablet_server.software_version(),
                tablet_server.seqno(),
                format!("{:?}", tablet_server.duration_since_heartbeat()),
            ]);
        }
        self.print_prettytable(table);
    }

    /// Prints the tablets.
    pub fn print_tablets(&mut self, table: &kudu::Table, tablets: Vec<kudu::TabletInfo>) {
        let mut titles = row![
            "Tablet ID",
            "Leader Tablet Server ID",
            "Leader RPC Addresses"
        ];

        let schema = table.schema();
        let partition_schema = table.partition_schema();
        for hash_schema in partition_schema.hash_partition_schemas() {
            let hash_columns = hash_schema
                .columns()
                .iter()
                .map(|&idx| schema.columns()[idx].name())
                .join(", ");
            titles.add_cell(cell!(format!("Hash({}) Partition", hash_columns)));
        }

        let has_range_partition = !partition_schema
            .range_partition_schema()
            .columns()
            .is_empty();

        if has_range_partition {
            let range_columns = partition_schema
                .range_partition_schema()
                .columns()
                .iter()
                .map(|&idx| schema.columns()[idx].name())
                .join(", ");
            titles.add_cell(cell!(format!("Range({}) Partition", range_columns)));
        }

        let mut table = table!();
        table.set_titles(titles);

        for tablet in tablets {
            let mut row = row![tablet.id()];
            if let Some(leader) = tablet
                .replicas()
                .iter()
                .find(|tablet| tablet.role() == kudu::RaftRole::Leader)
            {
                row.add_cell(cell!(leader.id()));
                row.add_cell(cell!(leader.rpc_addrs().iter().join(", ")));
            } else {
                row.add_cell(cell!(""));
                row.add_cell(cell!(""));
            }

            for partition in tablet.partition().hash_partitions() {
                row.add_cell(cell!(partition));
            }

            if has_range_partition {
                row.add_cell(cell!(format!("{:?}", RangePartition(tablet.partition()))));
            }
            table.add_row(row);
        }

        self.print_prettytable(table);
    }

    /// Prints the replicas.
    pub fn print_replicas(&mut self, tablets: Vec<kudu::TabletInfo>) {
        let mut table = table![["Tablet ID", "Tablet Server ID", "RPC Addresses", "Role",]];

        for tablet in tablets {
            for replica in tablet.replicas() {
                table.add_row(row![
                    tablet.id(),
                    replica.id(),
                    replica.rpc_addrs().iter().join(", "),
                    format!("{:?}", replica.role()),
                ]);
            }
        }

        self.print_prettytable(table);
    }

    /// Prints a table description to stdout.
    pub fn print_table_description(&mut self, schema: &kudu::Schema) {
        let mut table = table![["Column", "Type", "Nullable", "Encoding", "Compression",]];

        for column in schema.columns() {
            table.add_row(row![
                column.name(),
                format!("{:?}", column.data_type()),
                column.is_nullable(),
                format!("{:?}", column.encoding()),
                format!("{:?}", column.compression()),
            ]);
        }

        self.print_prettytable(table);
    }

    pub fn print_scan<'a>(
        &'a mut self,
        scan: kudu::Scan,
    ) -> impl Future<Item = (), Error = kudu::Error> + 'a {
        let mut table = prettytable::Table::new();
        table.set_titles(prettytable::row::Row::new(
            scan.projected_schema()
                .columns()
                .iter()
                .map(kudu::Column::name)
                .map(prettytable::cell::Cell::new)
                .collect::<Vec<_>>(),
        ));

        scan.fold(
            table,
            |mut table, batch| -> Result<prettytable::Table, kudu::Error> {
                for row in batch.into_iter() {
                    table.add_row(row_to_cell(&row)?);
                }
                Ok(table)
            },
        ).map(move |table| self.print_prettytable(table))
    }

    pub fn print_prettytable(&mut self, table: prettytable::Table) {
        table.print_term(&mut *self.out).unwrap();
    }

    pub fn print_create_table(&mut self, table: kudu::Table, tablets: Vec<kudu::TabletInfo>) {
        writeln!(&mut self.out, "CREATE TABLE {} (", table.name()).unwrap();
        writeln!(
            &mut self.out,
            "    {:?},",
            table.schema().columns().iter().format(",\n    ")
        ).unwrap();
        writeln!(
            &mut self.out,
            "    PRIMARY KEY ({}),\n)",
            table
                .schema()
                .primary_key()
                .iter()
                .map(kudu::Column::name)
                .format(", ")
        ).unwrap();

        let range_partitioned = !table
            .partition_schema()
            .range_partition_schema()
            .columns()
            .is_empty();
        let partitioned =
            range_partitioned || !table.partition_schema().hash_partition_schemas().is_empty();

        if partitioned {
            writeln!(&mut self.out, "PARTITION BY").unwrap();
        }

        let num_partitions = table.partition_schema().hash_partition_schemas().len()
            + if range_partitioned { 1 } else { 0 };

        for (i, hash_partition) in table
            .partition_schema()
            .hash_partition_schemas()
            .iter()
            .enumerate()
        {
            let seed = if hash_partition.seed() == 0 {
                String::new()
            } else {
                format!("SEED {} ", hash_partition.seed())
            };

            writeln!(
                &mut self.out,
                "    HASH ({}) {}PARTITIONS {}{}",
                hash_partition
                    .columns()
                    .iter()
                    .map(|&idx| table.schema().columns()[idx].name())
                    .format(", "),
                seed,
                hash_partition.num_buckets(),
                if i < num_partitions { "," } else { "" }
            ).unwrap();
        }

        if range_partitioned {
            writeln!(
                &mut self.out,
                "    RANGE ({}) (",
                table
                    .partition_schema()
                    .range_partition_schema()
                    .columns()
                    .iter()
                    .map(|&idx| table.schema().columns()[idx].name())
                    .format(", ")
            ).unwrap();

            let range_partitions = tablets
                .iter()
                .map(kudu::TabletInfo::partition)
                .filter(|partition| partition.hash_partitions().iter().all(|&b| b == 0))
                .format_with(",\n        ", |partition, f| {
                    f(&format_args!("PARTITION {:?}", RangePartition(partition)))
                });

            writeln!(&mut self.out, "        {},\n    )", range_partitions).unwrap();
        }

        writeln!(&mut self.out, "REPLICAS {};", table.num_replicas()).unwrap();
    }
}

/// Formats a range partition for printing.
struct RangePartition<'a>(&'a kudu::Partition);
impl<'a> fmt::Debug for RangePartition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_range_partition(f)
    }
}
