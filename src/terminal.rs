use std::fmt;
use std::io::Write;
use std::time::Duration;

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
            let mut line_end_idx = 0;
            for line in input.lines() {
                line_end_idx += 1 + line.len();
                if line_end_idx >= error_idx {
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
                    let arrow_idx = line.len() + 1 + error_idx - line_end_idx;
                    writeln!(self.err, "{:>2$}{:~<3$}", "", "^", arrow_idx, word_len).unwrap();
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

    pub fn print_warning(&mut self, msg: &str) {
        self.red();
        write!(self.err, "warning: ").unwrap();
        self.reset();
        writeln!(self.err, "{}", msg).unwrap();
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
    pub fn print_tablets(&mut self,
                         table: &kudu::Table,
                         tablets: Vec<kudu::Tablet>) {
        let schema = table.schema();
        let partition_schema = table.partition_schema();

        let mut columns = vec![
            vec!["Tablet ID".to_owned()],
            vec!["Leader Tablet Server ID".to_owned()],
            vec!["Leader RPC Addresses".to_owned()],
        ];
        let ids = 0;
        let leader_ids = 1;
        let leader_rpc_addrs = 2;

        for hash_schema in partition_schema.hash_partition_schemas() {
            let hash_columns = hash_schema.columns()
                                          .iter()
                                          .map(|&idx| schema.columns()[idx].name())
                                          .join(", ");
            columns.push(vec![format!("Hash({}) Partition", hash_columns)]);
        }

        let has_range_partition =
            !partition_schema.range_partition_schema().columns().is_empty();

        if has_range_partition {
            let range_columns = partition_schema.range_partition_schema()
                                                .columns()
                                                .iter()
                                                .map(|&idx| schema.columns()[idx].name())
                                                .join(", ");
            columns.push(vec![format!("Range({}) Partition", range_columns)]);
        }

        for tablet in tablets {
            columns[ids].push(tablet.id().to_string());

            let leader = tablet.replicas().iter().find(|tablet| tablet.role() == kudu::RaftRole::Leader);
            columns[leader_ids].push(leader.map(kudu::Replica::id)
                                           .map(kudu::TabletServerId::to_string)
                                           .unwrap_or(String::new()));
            columns[leader_rpc_addrs].push(leader.map(kudu::Replica::rpc_addrs)
                                                 .map(host_ports_to_string)
                                                 .unwrap_or(String::new()));

            for (offset, &partition) in tablet.partition().hash_partitions().iter().enumerate() {
                columns[leader_rpc_addrs + offset + 1].push(partition.to_string())
            }

            if has_range_partition {
                columns[leader_rpc_addrs + partition_schema.hash_partition_schemas().len() + 1].push(
                    format!("{:?}", RangePartition(tablet.partition()))
                );
            }
        }

        self.print_table(&columns);
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
        writeln!(&mut self.out, "CREATE TABLE {} (", table.name()).unwrap();
        writeln!(&mut self.out, "    {:?},",
                 table.schema()
                      .columns()
                      .iter()
                      .format(",\n    ")).unwrap();
        writeln!(&mut self.out, "    PRIMARY KEY ({}),\n)",
                 table.schema()
                      .primary_key()
                      .iter()
                      .map(kudu::Column::name)
                      .format(", ")).unwrap();

        let range_partitioned = !table.partition_schema().range_partition_schema().columns().is_empty();
        let partitioned = range_partitioned || !table.partition_schema().hash_partition_schemas().is_empty();

        if partitioned {
            writeln!(&mut self.out, "PARTITION BY").unwrap();
        }

        let num_partitions = table.partition_schema().hash_partition_schemas().len() +
                             if range_partitioned { 1 } else { 0 };

        for (i, hash_partition) in table.partition_schema().hash_partition_schemas().iter().enumerate() {
            let seed = if hash_partition.seed() == 0 {
                String::new()
            } else {
                format!("SEED {} ", hash_partition.seed())
            };

            writeln!(&mut self.out, "    HASH ({}) {}PARTITIONS {}{}",
                     hash_partition.columns()
                                   .iter()
                                   .map(|&idx| table.schema().columns()[idx].name())
                                   .format(", "),
                     seed,
                     hash_partition.num_buckets(),
                     if i < num_partitions { "," } else { "" }).unwrap();
        }

        if range_partitioned {
            writeln!(&mut self.out, "    RANGE ({}) (",
                     table.partition_schema()
                          .range_partition_schema()
                          .columns()
                          .iter()
                          .map(|&idx| table.schema().columns()[idx].name())
                          .format(", ")).unwrap();

            let range_partitions = tablets.iter()
                                          .map(kudu::Tablet::partition)
                                          .filter(|partition| partition.hash_partitions().iter().all(|&b| b == 0))
                                          .format_with(",\n        ", |partition, f| {
                                              f(&format_args!("PARTITION {:?}", RangePartition(partition)))
                                          });

            writeln!(&mut self.out, "        {},\n    )", range_partitions).unwrap();
        }

        writeln!(&mut self.out, "REPLICAS {};", table.num_replicas()).unwrap();
    }

    pub fn print_not_implemented(&mut self) {
        writeln!(&mut self.out, "not yet implemented!").unwrap();
    }
}

/// Formats a range partition for printing.
struct RangePartition<'a>(&'a kudu::Partition);
impl <'a> fmt::Debug for RangePartition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_range_partition(f)
    }
}
