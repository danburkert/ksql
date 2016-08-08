use std::fmt;
use std::io::Write;
use std::time::{Duration, UNIX_EPOCH, SystemTime};

use chrono;
use itertools::Itertools;
use kudu;
use libc;
use term;

use parser::Hint;
use Color;
use HELP;

pub struct Terminal {
    out: Box<term::StdoutTerminal>,
    err: Box<term::StderrTerminal>,
    color: bool,
}

fn host_ports_to_string(host_ports: &[(String, u16)]) -> String {
    let mut s = String::new();
    let mut is_first = true;
    for host_port in host_ports {
        if is_first { is_first = false; }
        else { s.push(',') };
        s.push_str(&host_port.0);
        s.push(':');
        s.push_str(&host_port.1.to_string());
    }

    s
}

fn format(n: u64, n_per_unit: u64, unit: &str) -> String {
    if n % n_per_unit == 0 {
        format!("{}{}", n / n_per_unit, unit)
    } else {
        format!("{:.2}{}", n as f64 / n_per_unit as f64, unit)
    }
}

fn duration_to_string(duration: Duration) -> String {
    let s = duration.as_secs();
    let ns = duration.subsec_nanos() as u64;

    const NANOS_PER_MICRO: u64 = 1_000;
    const NANOS_PER_MILLI: u64 = 1_000_000;
    const NANOS_PER_SEC: u64 = 1_000_000_000;
    const SECS_PER_MINUTE: u64 = 60;
    const SECS_PER_HOUR: u64 = 60 * 60;

    if s >= SECS_PER_HOUR {
        format(s, SECS_PER_HOUR, "h")
    } else if s >= SECS_PER_MINUTE {
        format(s, SECS_PER_MINUTE, "m")
    } else if s >= 1 {
        format(s * NANOS_PER_SEC + ns, NANOS_PER_SEC, "s")
    } else if ns >= NANOS_PER_MILLI {
        format(ns, NANOS_PER_MILLI, "ms")
    } else if ns >= 1_000 {
        format(ns, NANOS_PER_MICRO, "μs")
    } else {
        format!("{}ns", ns)
    }
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

    /// Prints a parse error to stderr.
    pub fn print_parse_error(&mut self, input: &str, remaining: &str, hints: &[Hint]) {
        let error_idx = input.len() - remaining.len();
        assert_eq!(&input[error_idx..], remaining);
        if !input.is_empty() {
            for line in input.lines() {

                if line.len() >= error_idx {
                    self.red();
                    write!(self.err, "error: ").unwrap();
                    self.reset();

                    let hints = hints.into_iter().map(|hint| {
                        match hint {
                            &Hint::Constant(ref expected) => format!("'{}'", *expected,),
                            &Hint::Integer => "an integer".to_string(),
                            &Hint::PosInteger => "a positive integer".to_string(),
                            &Hint::Float => "a floating point value".to_string(),
                            &Hint::Timestamp => "a timestamp value".to_string(),
                            &Hint::CharEscape => "a character escape sequence".to_string(),
                            &Hint::HexEscape => "a hex escape sequence".to_string(),
                            &Hint::Table(_) => "a table name".to_string(),
                            &Hint::Column(_) => "a column name".to_string(),
                        }
                    }).collect::<Vec<_>>();

                    self.bold();
                    writeln!(self.err, "expected: {}", hints.join(" or ")).unwrap();
                    self.reset();

                    let word_len = input[error_idx..].split_whitespace().next().unwrap_or("").len();
                    writeln!(self.err, "{}", line).unwrap();
                    writeln!(self.err, "{:>2$}{:~<3$}", "", "^", error_idx, word_len).unwrap();
                    break;
                }
            }
        }
        writeln!(&mut self.out, "").unwrap();
    }

    /// Prints a kudu err to stderr.
    pub fn print_kudu_error(&mut self, error: &kudu::Error) {
        self.red();
        write!(self.err, "error: ").unwrap();
        self.reset();
        writeln!(self.err, "{}", error).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_success(&mut self, msg: &str) {
        writeln!(self.out, "{}", msg).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_help(&mut self) {
        writeln!(&mut self.out, "{}", HELP).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    /// Prints the master servers.
    pub fn print_masters(&mut self, masters: Vec<kudu::Master>) {
        let mut ids = vec!["ID".to_owned()];
        let mut rpc_addrs = vec!["RPC Addresses".to_owned()];
        let mut http_addrs = vec!["HTTP Addresses".to_owned()];
        let mut seqnos = vec!["Sequence Number".to_owned()];
        let mut roles = vec!["Role".to_owned()];

        for master in masters {
            ids.push(master.id().to_string());
            rpc_addrs.push(host_ports_to_string(master.rpc_addrs()));
            http_addrs.push(host_ports_to_string(master.http_addrs()));
            seqnos.push(master.seqno().to_string());
            roles.push(format!("{:?}", master.role()));
        }

        self.print_table(&[&ids, &rpc_addrs, &http_addrs, &seqnos, &roles]);
    }

    /// Prints the tablet servers.
    pub fn print_tablet_servers(&mut self, tablet_servers: Vec<kudu::TabletServer>) {
        let mut ids = vec!["Tablet Server ID".to_owned()];
        let mut rpc_addrs = vec!["RPC Addresses".to_owned()];
        let mut http_addrs = vec!["HTTP Addresses".to_owned()];
        let mut versions = vec!["Software Version".to_owned()];
        let mut seqnos = vec!["Sequence Number".to_owned()];
        let mut heartbeats = vec!["Last Heartbeat".to_owned()];

        for tablet_server in tablet_servers {
            ids.push(tablet_server.id().to_string());
            rpc_addrs.push(host_ports_to_string(tablet_server.rpc_addrs()));
            http_addrs.push(host_ports_to_string(tablet_server.http_addrs()));
            versions.push(tablet_server.software_version().to_owned());
            seqnos.push(tablet_server.seqno().to_string());
            heartbeats.push(duration_to_string(tablet_server.duration_since_heartbeat()));
        }

        self.print_table(&[&ids, &rpc_addrs, &http_addrs, &versions, &seqnos, &heartbeats]);
    }

    /// Prints the tablets.
    pub fn print_tablets(&mut self, tablets: Vec<kudu::Tablet>) {
        let mut ids = vec!["Tablet ID".to_owned()];
        let mut lower_bounds = vec!["Partition Lower Bound".to_owned()];
        let mut upper_bounds = vec!["Partition Upper Bound".to_owned()];
        let mut leader_ids = vec!["Leader Tablet Server ID".to_owned()];
        let mut leader_rpc_addrs = vec!["Leader RPC Addresses".to_owned()];

        for tablet in tablets {
            ids.push(tablet.id().to_string());
            lower_bounds.push(format!("{:?}", tablet.partition().lower_bound()));
            upper_bounds.push(format!("{:?}", tablet.partition().upper_bound()));

            let leader = tablet.replicas().iter().find(|tablet| tablet.role() == kudu::RaftRole::Leader);
            leader_ids.push(leader.map(kudu::Replica::id)
                                  .map(kudu::TabletServerId::to_string)
                                  .unwrap_or(String::new()));
            leader_rpc_addrs.push(leader.map(kudu::Replica::rpc_addrs)
                                        .map(host_ports_to_string)
                                        .unwrap_or(String::new()));
        }

        self.print_table(&[&ids, &lower_bounds, &upper_bounds, &leader_ids, &leader_rpc_addrs]);
    }

    /// Prints the replicas.
    pub fn print_replicas(&mut self, tablets: Vec<kudu::Tablet>) {
        let mut tablet_ids = vec!["Tablet ID".to_owned()];
        let mut tablet_server_ids = vec!["Tablet Server ID".to_owned()];
        let mut rpc_addrs = vec!["RPC Addresses".to_owned()];
        let mut roles = vec!["Role".to_owned()];

        for tablet in tablets {
            for replica in tablet.replicas() {
                tablet_ids.push(tablet.id().to_string());
                tablet_server_ids.push(replica.id().to_string());
                rpc_addrs.push(host_ports_to_string(replica.rpc_addrs()));
                roles.push(format!("{:?}", replica.role()));
            }
        }

        self.print_table(&[&tablet_ids, &tablet_server_ids, &rpc_addrs, &roles]);
    }

    /// Prints a table description to stdout.
    pub fn print_table_description(&mut self, schema: &kudu::Schema) {
        let columns = (0..schema.columns().len()).map(|idx| schema.column(idx).unwrap()).collect::<Vec<_>>();

        let mut names = vec!["Column".to_owned()];
        names.extend(columns.iter().map(|col| col.name().to_owned()));

        let mut types = vec!["Type".to_owned()];
        types.extend(columns.iter().map(|col| format!("{:?}", col.data_type())));

        let mut nullables = vec!["Nullable".to_owned()];
        nullables.extend(columns.iter().map(|col| {
            if col.is_nullable() { "True".to_owned() } else { "False".to_owned() }
        }));

        let mut encodings = vec!["Encoding".to_owned()];
        encodings.extend(columns.iter().map(|col| format!("{:?}", col.encoding())));

        let mut compressions = vec!["Compression".to_owned()];
        compressions.extend(columns.iter().map(|col| format!("{:?}", col.compression())));

        self.print_table(&[&names, &types, &nullables, &encodings, &compressions]);
    }

    pub fn print_table<C>(&mut self, columns: &[C]) where C: AsRef<[String]> {
        let rows = columns.iter().fold(None, |prev, col| {
            let col = col.as_ref();
            let rows = col.len();
            if let Some(prev_rows) = prev {
                assert_eq!(prev_rows, rows);
            }
            Some(rows)
        }).expect("there must be at least a header");
        let widths = columns.iter()
                            .map(|col| col.as_ref().iter().map(|cell| cell.len()).max().unwrap_or(0))
                            .collect::<Vec<_>>();

        for row in 0..rows {
            for col in 0..columns.len() {
                if col == 0 {
                    write!(&mut self.out, "{:<1$} ", columns[col].as_ref()[row], widths[col]).unwrap();
                } else if col == columns.len() - 1 {
                    write!(&mut self.out, "| {:<1$}", columns[col].as_ref()[row], widths[col]).unwrap();
                } else {
                    write!(&mut self.out, "| {:<1$} ", columns[col].as_ref()[row], widths[col]).unwrap();
                }
            }
            writeln!(&mut self.out, "").unwrap();

            if row == 0 {
                for col in 0..columns.len() {
                    if col == 0 {
                        write!(&mut self.out, "{:-<1$}", "", widths[col]).unwrap();
                    } else {
                        write!(&mut self.out, "-+-{:-<1$}", "", widths[col]).unwrap();
                    }
                }
                writeln!(&mut self.out, "").unwrap();
            }
        }
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_create_table(&mut self, table: kudu::Table, tablets: Vec<kudu::Tablet>) {
        writeln!(&mut self.out, "CREATE TABLE {:?} (", table.name()).unwrap();
        writeln!(&mut self.out, "\t{:?}",
                 table.schema()
                      .columns()
                      .iter()
                      .format_default(",\n\t")).unwrap();
        writeln!(&mut self.out, ") PRIMARY KEY ({:?})",
                 table.schema()
                      .primary_key()
                      .iter()
                      .map(kudu::Column::name)
                      .format_default(", ")).unwrap();
        writeln!(&mut self.out, "DISTRIBUTE BY").unwrap();

        if table.partition_schema().range_partition_schema().columns().len() > 0 {
            writeln!(&mut self.out, "\tRANGE ({:?})",
                     table.partition_schema()
                          .range_partition_schema()
                          .columns()
                          .iter()
                          .map(|&idx| table.schema().columns()[idx].name())
                          .format_default(", ")).unwrap();

            let bounds = tablets.iter()
                                .map(kudu::Tablet::partition)
                                .filter(|partition| partition.lower_bound().hash_buckets().iter().all(|&b| b == 0))
                                .format(",\n\t\t\t", |partition, f| f(&format_args!("(({:?}), ({:?}))",
                                                                             RangePartitionKey { partition_key: partition.lower_bound() },
                                                                             RangePartitionKey { partition_key: partition.upper_bound() })));


            writeln!(&mut self.out, "\t\tBOUNDS  {}", bounds).unwrap();
        }

        for hash_partition in table.partition_schema().hash_partition_schemas() {
            let seed = if hash_partition.seed() == 0 {
                String::new()
            } else {
                format!("WITH SEED {} ", hash_partition.seed())
            };

            writeln!(&mut self.out, "\tHASH ({:?}) {}INTO {} BUCKETS",
                     hash_partition.columns()
                                   .iter()
                                   .map(|&idx| table.schema().columns()[idx].name())
                                   .format_default(", "),
                     seed,
                     hash_partition.num_buckets()).unwrap();
        }

        writeln!(&mut self.out, "WITH {} REPLICAS;", table.num_replicas()).unwrap();
    }

    pub fn print_not_implemented(&mut self) {
        writeln!(&mut self.out, "not yet implemented!").unwrap();
    }
}

/// The range component of a partition key.
struct RangePartitionKey<'a> {
    partition_key: &'a kudu::PartitionKey,
}

impl <'a> fmt::Debug for RangePartitionKey<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut is_first = true;
        let row = &self.partition_key.range_row();
        for &idx in self.partition_key.partition_schema().range_partition_schema().columns() {
            if !row.is_set(idx).unwrap() { break; }
            if is_first { is_first = false; }
            else { try!(write!(f, ", ")) }
            let column = &row.schema().columns()[idx];
            match column.data_type() {
                kudu::DataType::Bool => try!(write!(f, "{}", row.get::<bool>(idx).unwrap())),
                kudu::DataType::Int8 => try!(write!(f, "{}", row.get::<i8>(idx).unwrap())),
                kudu::DataType::Int16 => try!(write!(f, "{}", row.get::<i16>(idx).unwrap())),
                kudu::DataType::Int32 => try!(write!(f, "{}", row.get::<i32>(idx).unwrap())),
                kudu::DataType::Int64 => try!(write!(f, "{}", row.get::<i64>(idx).unwrap())),
                kudu::DataType::Timestamp => try!(fmt_timestamp(f, row.get::<SystemTime>(idx).unwrap())),
                kudu::DataType::Float => try!(write!(f, "{}", row.get::<f32>(idx).unwrap())),
                kudu::DataType::Double => try!(write!(f, "{}", row.get::<f64>(idx).unwrap())),
                kudu::DataType::Binary => try!(fmt_hex(f, row.get::<&[u8]>(idx).unwrap())),
                kudu::DataType::String => try!(write!(f, "{:?}", row.get::<&str>(idx).unwrap())),
            }
        }
        Ok(())
    }
}

pub fn fmt_timestamp(f: &mut fmt::Formatter, timestamp: SystemTime) -> fmt::Result {
    let datetime = if timestamp < UNIX_EPOCH {
        chrono::NaiveDateTime::from_timestamp(0, 0) -
            chrono::Duration::from_std(UNIX_EPOCH.duration_since(timestamp).unwrap()).unwrap()
    } else {
        chrono::NaiveDateTime::from_timestamp(0, 0) +
            chrono::Duration::from_std(timestamp.duration_since(UNIX_EPOCH).unwrap()).unwrap()
    };

    write!(f, "{}", datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
}

pub fn fmt_hex<T>(f: &mut fmt::Formatter, bytes: &[T]) -> fmt::Result where T: fmt::LowerHex {
    if bytes.is_empty() {
        return write!(f, "0x")
    }
    try!(write!(f, "{:#x}", bytes[0]));
    for b in &bytes[1..] {
        try!(write!(f, "{:x}", b));
    }
    Ok(())
}
