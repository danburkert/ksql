use std::borrow::Cow;
use std::iter;
use std::time::{
    Duration,
    SystemTime,
    UNIX_EPOCH
};

use chrono;
use futures::{
    Future,
    Stream,
    future,
};
use kudu;
use tokio::runtime::current_thread::Runtime;

use terminal::Terminal;

/// A command parsed from user input. The root of the SQL AST.
#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    /// /* no-op */ ;
    Noop,

    /// HELP;
    Help,

    /// SHOW TABLES;
    ShowTables,

    /// SHOW CREATE TABLE <table>;
    ShowCreateTable {
        table: &'a str,
    },

    /// SHOW MASTERS;
    ShowMasters,

    /// SHOW TABLET SERVERS;
    ShowTabletServers,

    /// SHOW TABLETS OF TABLE <table>;
    ShowTableTablets {
        table: &'a str,
    },

    /// SHOW TABLET REPLICAS OF TABLE <table>;
    ShowTableReplicas {
        table: &'a str,
    },

    /// DESCRIBE TABLE <table>;
    DescribeTable {
        table: &'a str,
    },

    /// DROP TABLE <table>;
    DropTable {
        table: &'a str,
    },

    /// INSERT INTO <table> [(<col>, ..)] VALUES (<col-val>, ..), ..;
    Insert {
        table: &'a str,
        columns: Option<Vec<&'a str>>,
        rows: Vec<Vec<Literal<'a>>>,
    },

    /// SELECT * FROM <table>;
    /// SELECT <col>,.. FROM <table>;
    /// SELECT COUNT(*) FROM <table>;
    Select {
        table: &'a str,
        selector: Selector<'a>,
    },

    /// CREATE TABLE <table> (<col> <data-type> [NOT NULL] [PRIMARY KEY] [ENCODING <encoding>] [COMPRESSION <compression>] [BLOCK SIZE <block-size>], ..,
    /// PRIMARY KEY (<col>, ..))
    /// [DISTRIBUTE BY [RANGE (<col>, ..) [SPLIT ROWS (<col-val>, ..)[, (<col-val>, ..)..]]]
    ///                [HASH (<col>, ..) [WITH SEED <seed>] INTO <buckets> BUCKETS]..;
    CreateTable {
        name: &'a str,
        columns: Vec<ColumnSpec<'a>>,
        primary_key: Vec<&'a str>,
        range_partition: Option<RangePartition<'a>>,
        hash_partitions: Vec<HashPartition<'a>>,
        replicas: Option<u32>,
    },

    AlterTable {
        table_name: &'a str,
        steps: Vec<AlterTableStep<'a>>,
    },
}

impl <'a> Command<'a> {

    pub fn execute(self,
                   runtime: &mut Runtime,
                   client: &mut kudu::Client,
                   term: &mut Terminal) {
        match self {
            Command::Noop => (),
            Command::Help => term.print_help(),
            Command::ShowTables => match runtime.block_on(client.tables()) {
                Ok(tables) => {
                    let mut names = Vec::with_capacity(tables.len());
                    let mut ids = Vec::with_capacity(tables.len());
                    names.push("Table".to_string());
                    ids.push("ID".to_string());
                    for (table, id) in tables {
                        names.push(table);
                        ids.push(id.to_string());
                    }
                    term.print_table(&[names, ids])
                },
                Err(error) => term.print_kudu_error(&error),
            },
            Command::ShowCreateTable { table } => {
                match runtime.block_on(future::lazy(|| {
                    client.open_table(table)
                          .and_then(|table| {
                              table.tablets().collect().map(|tablets| (table, tablets))
                          })
                })) {
                    Ok((table, tablets)) => term.print_create_table(table, tablets),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::ShowMasters => match runtime.block_on(client.masters()) {
                Ok(masters) => term.print_masters(masters),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::ShowTabletServers => match runtime.block_on(client.tablet_servers()) {
                Ok(tablet_servers) => term.print_tablet_servers(tablet_servers),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::ShowTableTablets { table } => {
                match runtime.block_on(future::lazy(|| {
                    client.open_table(table)
                          .and_then(|table| {
                              table.tablets().collect().map(|tablets| (table, tablets))
                          })
                })) {
                    Ok((table, tablets)) => term.print_tablets(&table, tablets),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::ShowTableReplicas { table } => {
                match runtime.block_on(future::lazy(|| client.open_table(table)
                                                             .and_then(|table| table.tablets().collect()))) {
                    Ok(tablets) => term.print_replicas(tablets),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::DescribeTable { table } => match runtime.block_on(client.open_table(table)) {
                Ok(table) => term.print_table_description(table.schema()),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::DropTable { table } => {
                match runtime.block_on(client.delete_table(table)) {
                    Ok(_) => term.print_success("table dropped"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::Select { table, selector } => {
                let columns = match selector {
                    Selector::Star => None,
                    Selector::Columns(columns) => Some(columns),
                    Selector::CountStar => return match runtime.block_on(future::lazy(|| count(client, table))) {
                        Ok(table) => term.print_table(&table),
                        Err(error) => term.print_kudu_error(&error),
                    }
                };

                match runtime.block_on(future::lazy(|| {
                    scan(client, table, columns)
                        .for_each(|table| {
                            term.print_table(&table[..]);
                            Ok(())
                        })
                })) {
                    Ok(()) => (),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::Insert { table, columns, rows } => {
                // This AsRef shenanigans is pretty unfortunate, maybe when async/await is
                // available it will be more straightforward.
                match runtime.block_on(insert(client, table, columns, rows.iter().map(AsRef::as_ref))) {
                    Ok(stats) => term.print_success(&format!("{} rows inserted, {} rows failed",
                                                             stats.successful_operations(),
                                                             stats.failed_operations())),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::CreateTable { name, columns, primary_key, range_partition, hash_partitions, replicas } => {
                match runtime.block_on(create_table(client, name, columns, primary_key, range_partition, hash_partitions, replicas)) {
                    Ok(_) => term.print_success("table created"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::AlterTable { table_name, steps } => {
                eprintln!("Altering table {}; steps: {:?}", table_name, steps);
                match runtime.block_on(future::lazy(move || alter_table(client, table_name, steps))) {
                    Ok(_) => term.print_success("table altered"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
        }
    }
}

fn create_table<'a>(client: &'a mut kudu::Client,
                    name: &str,
                    mut columns: Vec<ColumnSpec>,
                    primary_key: Vec<&str>,
                    range_partition: Option<RangePartition>,
                    hash_partitions: Vec<HashPartition>,
                    replicas: Option<u32>)
                    -> impl Future<Item=(), Error=kudu::Error> + 'a {
    future::result((|| {
        let mut schema = kudu::SchemaBuilder::new();

        for column_name in &primary_key {
            if let Some(mut column) = columns.iter_mut().find(|column| &column.name() == column_name) {
                column.not_null = true;
            }
        }

        for column in columns {
            schema = schema.add_column(column.into_column());
        }
        let schema = schema.set_primary_key(primary_key).build()?;

        let mut table_builder = kudu::TableBuilder::new(name, schema.clone());

        if let Some(range_partition) = range_partition {
            table_builder.set_range_partition_columns(range_partition.columns.clone());

            // Find column by name in schema, then map into the index and type
            let mut columns = Vec::with_capacity(range_partition.columns.len());
            for column in range_partition.columns {
                let index = schema.column_index(column).unwrap();
                columns.push((index, schema.column(index).unwrap().data_type()));
            }

            for (lower_bound, upper_bound) in range_partition.partitions {
                let (lower_bound, upper_bound) = convert_bounds(&schema, &columns, lower_bound, upper_bound)?;
                table_builder.add_range_partition(lower_bound, upper_bound);
            }
        } else {
            table_builder.set_range_partition_columns(Vec::<String>::new());
        }

        for hash_partition in hash_partitions {
            table_builder.add_hash_partitions_with_seed(hash_partition.columns,
                                                        hash_partition.buckets,
                                                        hash_partition.seed.unwrap_or(0));
        }

        if let Some(replicas) = replicas {
            table_builder.set_num_replicas(replicas);
        }
        Ok(table_builder)
    })()).and_then(move |table_builder| client.create_table(table_builder))
         .map(|_| ())
}

fn alter_table<'a>(client: &'a mut kudu::Client,
                   table_name: &'a str,
                   steps: Vec<AlterTableStep<'a>>)
                   -> impl Future<Item=(), Error=kudu::Error> + 'a {
    client.open_table(table_name)
          .and_then(|table| {
            let mut builder = kudu::AlterTableBuilder::new();
            let mut columns = Vec::new();
            for &idx in table.partition_schema().range_partition_schema().columns() {
                columns.push((idx, table.schema().columns()[idx].data_type()));
            }

            for step in steps {
                match step {
                    AlterTableStep::RenameTable { table_name } => {
                        builder.rename_table(table_name);
                    },
                    AlterTableStep::RenameColumn { old_column_name, new_column_name } => {
                        builder.rename_column(old_column_name, new_column_name);
                    },
                    AlterTableStep::AddColumn { column } => {
                        builder.add_column(column.into_column());
                    },
                    AlterTableStep::DropColumn { column_name } => {
                        builder.drop_column(column_name);
                    },
                    AlterTableStep::AddRangePartition(lower_bound, upper_bound) => {
                        let (lower_bound, upper_bound) =
                            convert_bounds(table.schema(), &columns, lower_bound, upper_bound)?;
                        builder.add_range_partition(&lower_bound, &upper_bound);
                    },
                    AlterTableStep::DropRangePartition(lower_bound, upper_bound) => {
                        let (lower_bound, upper_bound) =
                            convert_bounds(table.schema(), &columns, lower_bound, upper_bound)?;
                        builder.drop_range_partition(&lower_bound, &upper_bound);
                    },
                }
            }
            Ok((table.id(), builder))
          })
          .and_then(move |(table_id, builder)| client.alter_table_by_id(table_id, builder))
          .map(|_| ())
}

fn convert_bounds<'a>(schema: &kudu::Schema,
                      columns: &[(usize, kudu::DataType)],
                      lower_bound: Bound<'a>,
                      upper_bound: Bound<'a>)
                      -> kudu::Result<(kudu::RangePartitionBound, kudu::RangePartitionBound)> {
    let lower_bound: kudu::RangePartitionBound = match lower_bound {
        Bound::Inclusive(values) => {
            kudu::RangePartitionBound::Inclusive(row(schema, columns, &values)?.into_owned())
        },
        Bound::Exclusive(values) => {
            kudu::RangePartitionBound::Exclusive(row(schema, columns, &values)?.into_owned())
        },
        Bound::Unbounded => kudu::RangePartitionBound::Inclusive(schema.new_row()),
    };
    let upper_bound: kudu::RangePartitionBound = match upper_bound {
        Bound::Inclusive(values) => {
            kudu::RangePartitionBound::Inclusive(row(schema, columns, &values)?.into_owned())
        },
        Bound::Exclusive(values) => {
            kudu::RangePartitionBound::Exclusive(row(schema, columns, &values)?.into_owned())
        },
        Bound::Unbounded => kudu::RangePartitionBound::Exclusive(schema.new_row()),
    };
    Ok((lower_bound, upper_bound))
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

    unsafe {
        String::from_utf8_unchecked(v)
    }
}

fn to_date_time(timestamp: &SystemTime) -> chrono::DateTime<chrono::Utc> {
    let naive = match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => chrono::NaiveDateTime::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos()),
        Err(err) => {
            let duration = err.duration();
            if duration.subsec_nanos() > 0 {
                chrono::NaiveDateTime::from_timestamp((-(duration.as_secs() as i64)) - 1, 1_000_000_000 - duration.subsec_nanos())
            } else {
                chrono::NaiveDateTime::from_timestamp(-(duration.as_secs() as i64), 0)
            }
        },
    };
    chrono::DateTime::from_utc(naive, chrono::Utc)
}

fn scan<'a>(client: &'a mut kudu::Client,
        table: &'a str,
        columns: Option<Vec<&'a str>>)
        -> impl Stream<Item=Vec<Vec<String>>, Error=kudu::Error> + 'a {

    client.open_table(table)
        .and_then(|table| {
            let mut builder = table.scan_builder();

            if let Some(columns) = columns {
                builder = builder.projected_column_names(columns)?;
            }

            Ok(builder.build())
        })
        .flatten_stream()
        .and_then(|batch| {
            let mut columns = batch.projected_schema()
                                   .columns()
                                   .iter()
                                   .map(|column| vec![column.name().to_owned()])
                                   .collect::<Vec<_>>();

            for row in batch.into_iter() {
                for (idx, mut column) in batch.projected_schema().columns().iter().enumerate() {
                    let val = if row.is_null(idx)? {
                        "null".to_owned()
                    } else {
                        match column.data_type() {
                            kudu::DataType::Bool => row.get::<bool>(idx)?.to_string(),
                            kudu::DataType::Int8 => row.get::<i8>(idx)?.to_string(),
                            kudu::DataType::Int16 => row.get::<i16>(idx)?.to_string(),
                            kudu::DataType::Int32 => row.get::<i32>(idx)?.to_string(),
                            kudu::DataType::Int64 => row.get::<i64>(idx)?.to_string(),
                            kudu::DataType::Float => row.get::<f32>(idx)?.to_string(),
                            kudu::DataType::Double => row.get::<f64>(idx)?.to_string(),
                            kudu::DataType::Timestamp => to_date_time(&row.get::<SystemTime>(idx)?).to_string(),
                            kudu::DataType::String => format!("{:?}", row.get::<&str>(idx)?),
                            kudu::DataType::Binary => to_hex(try!(row.get::<&[u8]>(idx))),
                        }
                    };
                    columns[idx].push(val);
                }
            }
            Ok(columns)
        })
}

fn count<'a>(client: &mut kudu::Client,
             table: &'a str)
             -> impl Future<Item=Vec<Vec<String>>, Error=kudu::Error> + 'a {
    client.open_table(table)
        .and_then(|table| {
            Ok(table.scan_builder()
                    .projected_columns(iter::empty())?
                    .build())
        })
        .and_then(|scan| {
            scan.fold(0, |acc, batch| Ok::<usize, kudu::Error>(acc + batch.num_rows()))
        })
        .map(|count| {
            vec![vec!["COUNT(*)".to_string(), count.to_string()]]
        })
}

fn insert<'a, Rows>(client: &mut kudu::Client,
                    table: &'a str,
                    columns: Option<Vec<&'a str>>,
                    rows: Rows)
              -> impl Future<Item=kudu::FlushStats, Error=kudu::Error> + 'a
where Rows: Iterator<Item=&'a [Literal<'a>]> +'a {
    client.open_table(table)
          .and_then(move |table| {
              // Find column by name in schema, then map into the index and type
              let num_columns = columns.as_ref().map(|cols| cols.len()).unwrap_or(table.schema().columns().len());
              let mut column_types = Vec::with_capacity(num_columns);
              if let Some(columns) = columns {
                  for column in columns {
                      let index = match table.schema().column_index(column) {
                          Some(index) => index,
                          None => return Err(kudu::Error::InvalidArgument(
                                  format!("column {:?} not found", column))),
                      };
                      column_types.push((index, table.schema().columns()[index].data_type()));
                  }
              } else {
                  for (index, column) in table.schema().columns().iter().enumerate() {
                      column_types.push((index, column.data_type()));
                  }
              }

              rows.map(|r| row(table.schema(), &column_types, r))
                  .collect::<Result<Vec<_>, _>>()
                  .map(move |rows| (table, rows))
          })
          .and_then(|(table, rows)| {
              let writer = table.new_writer(kudu::WriterConfig::default());
              writer.insert_all(rows)
          })
          .and_then(|writer| writer.flush())
          .map(|(_, flush_stats)| flush_stats)
}

/// Sets the columns in the partial row to the provided literal values.
fn row<'a>(schema: &kudu::Schema,
           columns: &[(usize, kudu::DataType)],
           values: &'a [Literal])
           -> kudu::Result<kudu::Row<'a>> {
    let mut row = schema.new_row();
    for (&(column_idx, data_type), value) in columns.iter().zip(values) {
        match (data_type, value) {
            (_, &Literal::Bool(b)) => row.set(column_idx, b)?,
            (kudu::DataType::Int8, &Literal::Integer(i)) => row.set(column_idx, i as i8)?,
            (kudu::DataType::Int16, &Literal::Integer(i)) => row.set(column_idx, i as i16)?,
            (kudu::DataType::Int32, &Literal::Integer(i)) => row.set(column_idx, i as i32)?,
            (_, &Literal::Integer(i)) => row.set(column_idx, i as i64)?,
            (_, &Literal::Timestamp(t)) => {
                // TODO: is this right?
                let value = t.timestamp() * 1_000_000 + i64::from(t.timestamp_subsec_micros());
                let time = if value < 0 {
                    let value = !(value as u64) + 1;
                    UNIX_EPOCH - Duration::new(value / 1000_000, (value % 1000_000) as u32 * 1000)
                } else {
                    let value = value as u64;
                    UNIX_EPOCH + Duration::new(value / 1000_000, (value % 1000_000) as u32 * 1000)
                };
                row.set(column_idx, time)?
            },
            (kudu::DataType::Float, &Literal::Float(f)) => row.set(column_idx, f as f32)?,
            (_, &Literal::Float(f)) => row.set(column_idx, f)?,
            (_, &Literal::String(ref s)) => row.set(column_idx, &s[..])?,
            (_, &Literal::Binary(ref b)) => row.set(column_idx, &b[..])?,
            (_, &Literal::Null) => row.set_null(column_idx)?,
        };
    };
    Ok(row)
}

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnSpec<'a> {
    name: &'a str,
    data_type: kudu::DataType,
    not_null: bool,
    encoding_type: Option<kudu::EncodingType>,
    compression_type: Option<kudu::CompressionType>,
    block_size: Option<u32>,
}

impl <'a> ColumnSpec<'a> {
    pub fn new(name: &'a str,
               data_type: kudu::DataType,
               not_null: bool,
               encoding_type: Option<kudu::EncodingType>,
               compression_type: Option<kudu::CompressionType>,
               block_size: Option<u32>)
               -> ColumnSpec<'a> {
        ColumnSpec {
            name: name,
            data_type: data_type,
            not_null: not_null,
            encoding_type: encoding_type,
            compression_type: compression_type,
            block_size: block_size,
        }
    }

    pub fn name(&self) -> &'a str {
        self.name
    }

    pub fn into_column(&self) -> kudu::Column {
        let mut column = kudu::Column::builder(self.name, self.data_type);
        if self.not_null {
            column = column.set_not_null();
        }
        if let Some(encoding_type) = self.encoding_type {
            column = column.set_encoding(encoding_type);
        }
        if let Some(compression_type) = self.compression_type {
            column = column.set_compression(compression_type);
        }
        if let Some(block_size) = self.block_size {
            column = column.set_block_size(block_size);
        }
        column
    }
}

#[derive(Debug, PartialEq)]
pub enum Bound<'a> {
    Inclusive(Vec<Literal<'a>>),
    Exclusive(Vec<Literal<'a>>),
    Unbounded,
}

#[derive(Debug, PartialEq)]
pub struct RangePartition<'a> {
    columns: Vec<&'a str>,
    partitions: Vec<(Bound<'a>, Bound<'a>)>,
}

impl <'a> RangePartition<'a> {
    pub fn new(columns: Vec<&'a str>,
               partitions: Vec<(Bound<'a>, Bound<'a>)>)
               -> RangePartition<'a> {
        RangePartition { columns: columns, partitions: partitions }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashPartition<'a> {
    columns: Vec<&'a str>,
    seed: Option<u32>,
    buckets: u32,
}

impl <'a> HashPartition<'a> {
    pub fn new(columns: Vec<&'a str>, seed: Option<u32>, buckets: u32) -> HashPartition<'a> {
        HashPartition { columns: columns, seed: seed, buckets: buckets }
    }
}

#[derive(Debug, PartialEq)]
pub enum AlterTableStep<'a> {
    RenameTable {
        table_name: &'a str
    },
    RenameColumn {
        old_column_name: &'a str,
        new_column_name: &'a str,
    },
    AddColumn {
        column: ColumnSpec<'a>,
    },
    DropColumn {
        column_name: &'a str,
    },
    AddRangePartition(Bound<'a>, Bound<'a>),
    DropRangePartition(Bound<'a>, Bound<'a>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Selector<'a> {
    Star,
    Columns(Vec<&'a str>),
    CountStar,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal<'a> {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    String(Cow<'a, str>),
    Binary(Vec<u8>),
    Null,
}
