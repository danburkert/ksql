use std::borrow::Cow;
use std::time::{
    Duration,
    Instant,
    SystemTime,
    UNIX_EPOCH
};

use chrono;
use kudu;

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

    /// DESCRIBE TABLE <table>;
    DescribeTable {
        table: &'a str,
    },

    /// DROP TABLE <table>;
    DropTable {
        table: &'a str,
    },

    Insert {
        table: &'a str,
        columns: Option<Vec<&'a str>>,
        rows: Vec<Vec<Literal<'a>>>,
    },

    Select {
        table: &'a str,
        selector: Selector<'a>,
    },

    /// CREATE TABLE <table> (<col> <data-type> [NULLABLE | NOT NULL] [ENCODING <encoding>] [COMPRESSION <compression>] [BLOCK SIZE <block-size>], ..)
    /// PRIMARY KEY (<col>, ..)
    /// [DISTRIBUTE BY [RANGE (<col>, ..) [SPLIT ROWS (<col-val>, ..)[, (<col-val>, ..)..]]]
    ///                [HASH (<col>, ..) [WITH SEED <seed>] INTO <buckets> BUCKETS]..;
    CreateTable {
        name: &'a str,
        columns: Vec<CreateColumn<'a>>,
        primary_key: Vec<&'a str>,
        range_partition: Option<RangePartition<'a>>,
        hash_partitions: Vec<HashPartition<'a>>,
        replicas: Option<i32>,
    }
}

impl <'a> Command<'a> {

    pub fn execute(self, client: &mut kudu::Client, term: &mut Terminal) {
        match self {
            Command::Noop => (),
            Command::Help => term.print_help(),
            Command::ShowTables => {
                match client.list_tables(Instant::now() + Duration::from_secs(10)) {
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
                }
            },
            Command::DescribeTable { table } => {
                match client.open_table(table, Instant::now() + Duration::from_secs(10)) {
                    Ok(table) => term.print_table_description(table.schema()),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::DropTable { table } => {
                match client.delete_table(table, Instant::now() + Duration::from_secs(10)) {
                    Ok(_) => term.print_success("table dropped"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::Select { table, selector } => {
                /*
                match select(client, table, selector) {
                    Ok(table) => term.print_table(&table[..]),
                    Err(error) => term.print_kudu_error(&error),
                }
                */
                term.print_not_implemented()
            },
            Command::Insert { table, columns, rows } => {
                /*
                let count = rows.len();
                match insert(client, table, columns, rows) {
                    Ok(_) => term.print_success(&format!("{} rows inserted", count)),
                    Err(error) => term.print_kudu_error(&error),
                }
                */
                term.print_not_implemented()
            },
            Command::CreateTable { name, columns, primary_key, range_partition, hash_partitions, replicas } => {
                match create_table(client, name, columns, primary_key, range_partition, hash_partitions, replicas) {
                    Ok(_) => term.print_success("table created"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
        }
    }
}

fn create_table<'a>(client: &mut kudu::Client,
                    name: &str,
                    columns: Vec<CreateColumn>,
                    primary_key: Vec<&str>,
                    range_partition: Option<RangePartition<'a>>,
                    hash_partitions: Vec<HashPartition>,
                    replicas: Option<i32>)
                    -> kudu::Result<()> {
    let mut schema = kudu::SchemaBuilder::new();
    for column in columns {
        let mut builder = schema.add_column(column.name, column.data_type);
        if let Some(false) = column.nullable {
            builder.set_not_null();
        }
        if let Some(encoding_type) = column.encoding_type {
            builder.set_encoding(encoding_type);
        }
        if let Some(compression_type) = column.compression_type {
            builder.set_compression(compression_type);
        }
        if let Some(block_size) = column.block_size {
            builder.set_block_size(block_size);
        }
    }
    schema.set_primary_key(primary_key.iter().map(ToString::to_string).collect::<Vec<_>>());
    let schema = try!(schema.build());

    let mut table_builder = kudu::TableBuilder::new(name, schema.clone());

    if let Some(range_partition) = range_partition {
        table_builder.set_range_partition_columns(range_partition.columns
                                                                 .iter()
                                                                 .map(ToString::to_string)
                                                                 .collect::<Vec<_>>());

        // Find column by name in schema, then map into the index and type
        let mut columns = Vec::with_capacity(range_partition.columns.len());
        for column in &range_partition.columns {
            let index = table_builder.schema().column_index(column).unwrap();
            columns.push((index, table_builder.schema().column(index).unwrap().data_type()));
        }

        //let rows = try!(build_rows(&schema, &columns, &range_partition.split_rows));

        // Find column by name in schema, then map into the index and type
        let mut columns = Vec::with_capacity(range_partition.columns.len());
        for column in range_partition.columns {
            let index = schema.column_index(column).unwrap();
            columns.push((index, schema.column(index).unwrap().data_type()));
        }

        for range_split in &range_partition.split_rows {
            let mut row = schema.new_row();
            try!(populate_row(&mut row, &columns, range_split));
            table_builder.add_range_split(row);
        }
    }

    for hash_partition in hash_partitions {
        table_builder.add_hash_partitions_with_seed(hash_partition.columns.iter().map(ToString::to_string).collect::<Vec<_>>(),
                                                    hash_partition.buckets,
                                                    hash_partition.seed.unwrap_or(0));
    }

    if let Some(replicas) = replicas {
        table_builder.set_num_replicas(replicas);
    }

    client.create_table(table_builder, Instant::now() + Duration::from_secs(10)).map(|_| ())
}

/*
fn select(client: &mut kudu::Client,
          table: &str,
          selector: Selector)
          -> kudu::Result<Vec<Vec<String>>> {
    match selector {
        Selector::Star => scan(client, table, None),
        Selector::Columns(columns) => scan(client, table, Some(columns)),
        Selector::CountStar => count(client, table),
    }
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

fn to_date_time(timestamp: &SystemTime) -> chrono::DateTime<chrono::UTC> {
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
    chrono::DateTime::from_utc(naive, chrono::UTC)
}

fn scan(client: &mut kudu::Client,
        table: &str,
        columns: Option<Vec<&str>>)
        -> kudu::Result<Vec<Vec<String>>> {
    let schema = try!(client.get_table_schema(table));
    let table = try!(client.open_table(table));

    let mut scanner = {
        let mut builder = kudu::ScanBuilder::new(&table);
        if let Some(ref columns) = columns { try!(builder.set_projected_column_names(&columns)); }
        try!(builder.build())
    };

    let mut columns: Vec<Vec<String>> = match columns {
        Some(columns) => columns.into_iter().map(|c| vec![c.to_owned()]).collect(),
        None => (0..schema.num_columns()).map(|idx| vec![schema.column(idx).name().to_owned()]).collect(),
    };

    let mut types = Vec::with_capacity(columns.len());
    for column in &columns {
        let index = try!(schema.find_column(&column[0]));
        types.push(schema.column(index).data_type());
    }

    let mut batch = kudu::ScanBatch::new();
    while scanner.has_more_rows() {
        batch = try!(scanner.next_batch(batch));
        for i in 0..batch.len() {
            let row = unsafe { batch.get_unchecked(i) };
            for (idx, (mut column, &data_type)) in columns.iter_mut().zip(types.iter()).enumerate() {
                let val = if row.is_null(idx) {
                    "null".to_owned()
                } else {
                    match data_type {
                        kudu::DataType::Bool => try!(row.get::<bool>(idx)).to_string(),
                        kudu::DataType::Int8 => try!(row.get::<i8>(idx)).to_string(),
                        kudu::DataType::Int16 => try!(row.get::<i16>(idx)).to_string(),
                        kudu::DataType::Int32 => try!(row.get::<i32>(idx)).to_string(),
                        kudu::DataType::Int64 => try!(row.get::<i64>(idx)).to_string(),
                        kudu::DataType::Float => try!(row.get::<f32>(idx)).to_string(),
                        kudu::DataType::Double => try!(row.get::<f64>(idx)).to_string(),
                        kudu::DataType::Timestamp => to_date_time(&try!(row.get::<SystemTime>(idx))).to_string(),
                        kudu::DataType::String => format!("{:?}", try!(row.get::<&str>(idx))),
                        kudu::DataType::Binary => to_hex(try!(row.get::<&[u8]>(idx))),
                    }
                };
                column.push(val);
            }
        }
    }
    Ok(columns)
    Ok(vec![vec!["not yet implemented"]])
}

fn count(client: &mut kudu::Client, table: &str) -> kudu::Result<Vec<Vec<String>>> {
    let table = try!(client.open_table(table));
    let mut scanner = {
        let mut builder = kudu::ScanBuilder::new(&table);
        try!(builder.set_projected_column_indexes(&[]));
        try!(builder.build())
    };

    let mut batch = kudu::ScanBatch::new();
    let mut count = 0;
    while scanner.has_more_rows() {
        batch = try!(scanner.next_batch(batch));
        count += batch.len();
    }
    Ok(vec![vec!["COUNT(*)".to_string(), count.to_string()]])
}

fn insert(client: &mut kudu::Client,
          table: &str,
          columns: Option<Vec<&str>>,
          rows: Vec<Vec<Literal>>)
          -> kudu::Result<()> {
    let schema = try!(client.get_table_schema(table));
    let mut session = client.new_session();
    session.set_timeout(&Duration::from_secs(60));
    try!(session.set_flush_mode(kudu::FlushMode::ManualFlush));

    // Find column by name in schema, then map into the index and type
    let num_columns = columns.as_ref().map(|cols| cols.len()).unwrap_or(schema.num_columns());
    let mut column_types = Vec::with_capacity(num_columns);
    if let Some(columns) = columns {
        for column in columns {
            let index = try!(schema.find_column(column));
            column_types.push((index, schema.column(index).data_type()));
        }
    } else {
        for index in 0..schema.num_columns() {
            column_types.push((index, schema.column(index).data_type()));
        }
    }

    let table = try!(client.open_table(table));

    for row in rows {
        if session.count_buffered_operations() > 1000 {
            try!(session.flush());
        };
        let mut insert = table.new_insert();
        try!(populate_row(&mut insert.row(), &column_types, &row));
        try!(session.insert(insert))
    }

    try!(session.flush());
    session.close()
}

*/

/// Sets the columns in the partial row to the provided literal values.
fn populate_row<'a>(row: &mut kudu::Row,
                    columns: &[(usize, kudu::DataType)],
                    values: &'a [Literal])
                    -> kudu::Result<()> {
    for (&(column_idx, data_type), value) in columns.iter().zip(values) {
        match (data_type, value) {
            (_, &Literal::Bool(b)) => try!(row.set(column_idx, b)),
            (kudu::DataType::Int8, &Literal::Integer(i)) => try!(row.set(column_idx, i as i8)),
            (kudu::DataType::Int16, &Literal::Integer(i)) => try!(row.set(column_idx, i as i16)),
            (kudu::DataType::Int32, &Literal::Integer(i)) => try!(row.set(column_idx, i as i32)),
            (_, &Literal::Integer(i)) => try!(row.set(column_idx, i as i64)),
            (_, &Literal::Timestamp(t)) => {
                let epoch: chrono::DateTime<chrono::UTC> = chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(0, 0), chrono::UTC);
                let value = (t - epoch).num_microseconds().unwrap();
                let time = if value < 0 {
                    let value = !(value as u64) + 1;
                    UNIX_EPOCH - Duration::new(value / 1000_000, (value % 1000_000) as u32 * 1000)
                } else {
                    let value = value as u64;
                    UNIX_EPOCH + Duration::new(value / 1000_000, (value % 1000_000) as u32 * 1000)
                };
                // TODO: switch back to SystemTime
                //try!(row.set(column_idx, time))
                try!(row.set(column_idx, value))
            },
            (kudu::DataType::Float, &Literal::Float(f)) => try!(row.set(column_idx, f as f32)),
            (_, &Literal::Float(f)) => try!(row.set(column_idx, f)),
            (_, &Literal::String(ref s)) => try!(row.set(column_idx, &s[..])),
            (_, &Literal::Binary(ref b)) => try!(row.set(column_idx, &b[..])),
            // TODO: add Row::set_null
            (_, &Literal::Null) => try!(row.set::<Option<i32>>(column_idx, None)),
        };
    };
    Ok(())
}

fn build_rows(schema: &kudu::Schema,
              columns: &[(usize, kudu::DataType)],
              literal_rows: &[Vec<Literal>])
              -> kudu::Result<Vec<kudu::Row>> {
    let mut rows = Vec::with_capacity(literal_rows.len());
    for literal_columns in literal_rows {
        let mut row: kudu::Row = schema.new_row();
        try!(populate_row(&mut row, columns, literal_columns));
        rows.push(row);
    };
    Ok(rows)
}

#[derive(Debug, PartialEq, Eq)]
pub struct CreateColumn<'a> {
    name: &'a str,
    data_type: kudu::DataType,
    nullable: Option<bool>,
    encoding_type: Option<kudu::EncodingType>,
    compression_type: Option<kudu::CompressionType>,
    block_size: Option<i32>,
}

impl <'a> CreateColumn<'a> {
    pub fn new(name: &'a str,
               data_type: kudu::DataType,
               nullable: Option<bool>,
               encoding_type: Option<kudu::EncodingType>,
               compression_type: Option<kudu::CompressionType>,
               block_size: Option<i32>)
               -> CreateColumn<'a> {
        CreateColumn {
            name: name,
            data_type: data_type,
            nullable: nullable,
            encoding_type: encoding_type,
            compression_type: compression_type,
            block_size: block_size,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RangePartition<'a> {
    columns: Vec<&'a str>,
    split_rows: Vec<Vec<Literal<'a>>>,
}

impl <'a> RangePartition<'a> {
    pub fn new(columns: Vec<&'a str>, split_rows: Vec<Vec<Literal<'a>>>) -> RangePartition<'a> {
        RangePartition { columns: columns, split_rows: split_rows }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashPartition<'a> {
    columns: Vec<&'a str>,
    seed: Option<u32>,
    buckets: i32,
}

impl <'a> HashPartition<'a> {
    pub fn new(columns: Vec<&'a str>, seed: Option<u32>, buckets: i32) -> HashPartition<'a> {
        HashPartition { columns: columns, seed: seed, buckets: buckets }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Selector<'a> {
    Star,
    Columns(Vec<&'a str>),
    CountStar,
}

#[derive(Debug, PartialEq)]
pub enum Literal<'a> {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    String(Cow<'a, str>),
    Binary(Vec<u8>),
    Null,
}
