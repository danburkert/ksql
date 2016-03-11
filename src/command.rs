use std::borrow::Cow;
use std::time::{Duration, UNIX_EPOCH};

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

    CreateTable {
        name: &'a str,
        columns: Vec<CreateColumn<'a>>,
        primary_key: Vec<&'a str>,
        range_partition: Option<RangePartition<'a>>,
        hash_partitions: Vec<HashPartition<'a>>,
    }
}

impl <'a> Command<'a> {

    pub fn execute(self, client: &mut kudu::Client, term: &mut Terminal) {
        match self {
            Command::Noop => (),
            Command::Help => term.print_help(),
            Command::ShowTables => {
                match client.list_tables() {
                    Ok(tables) => term.print_table_list(&tables),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::DescribeTable { table } => {
                match client.table_schema(table) {
                    Ok(schema) => term.print_table_description(&schema),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::CreateTable { name, columns, primary_key, range_partition, hash_partitions } => {
                match create_table(client, name, columns, primary_key, range_partition, hash_partitions) {
                    Ok(_) => term.print_success("table created"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
        }
    }
}

fn create_table(client: &mut kudu::Client,
                name: &str,
                columns: Vec<CreateColumn>,
                primary_key: Vec<&str>,
                range_partition: Option<RangePartition>,
                hash_partitions: Vec<HashPartition>)
                -> kudu::Result<()> {
    let mut schema = kudu::SchemaBuilder::new();
    for column in columns {
        let mut builder = schema.add_column(column.name);
        builder.data_type(column.data_type);
        if let Some(nullable) = column.nullable {
            builder.nullable(nullable);
        }
        if let Some(encoding_type) = column.encoding_type {
            builder.encoding_type(encoding_type);
        }
        if let Some(compression_type) = column.compression_type {
            builder.compression_type(compression_type);
        }
        if let Some(block_size) = column.block_size {
            builder.block_size(block_size);
        }
    }
    schema.set_primary_key_columns(&primary_key);

    let mut creator = client.new_table_creator();
    creator.table_name(name);
    let schema = try!(schema.build());
    creator.schema(&schema);

    if let Some(range_partition) = range_partition {
        creator.set_range_partition_columns(&range_partition.columns);
        let rows = try!(build_rows(&schema, &range_partition.split_rows));
        for row in rows {
            creator.add_split_row(row);
        }
    }

    for hash_partition in hash_partitions {
        creator.add_hash_partitions(&hash_partition.columns,
                                    hash_partition.buckets,
                                    hash_partition.seed.unwrap_or(0));
    }

    creator.create()
}

fn build_rows<'a>(schema: &'a kudu::Schema, values: &'a [Vec<Literal>]) -> kudu::Result<Vec<kudu::PartialRow<'a>>> {
    let columns: Vec<(usize, kudu::DataType)> = (0..schema.num_columns()).map(|column_idx| {
        (column_idx, schema.column(column_idx).data_type())
    }).collect();

    let mut rows = Vec::with_capacity(values.len());

    for column_values in values {
        if columns.len() != column_values.len() { panic!("wrong number of column values") };
        let mut row: kudu::PartialRow<'a> = schema.new_row();
        for (&(column_idx, data_type), value) in columns.iter().zip(column_values) {
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
                    try!(row.set(column_idx, time))
                },
                (kudu::DataType::Float, &Literal::Float(f)) => try!(row.set(column_idx, f as f32)),
                (_, &Literal::Float(f)) => try!(row.set(column_idx, f)),
                (_, &Literal::String(ref s)) => try!(row.set(column_idx, &s[..])),
                (_, &Literal::Binary(ref b)) => try!(row.set(column_idx, &b[..])),
            };
        };
        rows.push(row);
    };

    Ok(rows)
}

/*
fn build_row<'a>(schema: &'a kudu::Schema, values: &'a [Literal]) -> kudu::Result<kudu::PartialRow<'a>> {
    let row = schema.new_row();
    for (column_idx, value) in values.iter().enumerate() {
        match value {
            &Literal::Bool(b) => {
                try!(row.set(column_idx, b))
            },
            &Literal::Integer(i) => {
                match schema.column(column_idx).data_type() {
                    kudu::DataType::Int8 => row.set(column_idx, i as i8),
                    kudu::DataType::Int16 => row.set(column_idx, i as i16),
                    kudu::DataType::Int32 => row.set(column_idx, i as i32),
                    _ => try!(row.set(column_idx, i)),
                }
            },
            &Literal::Float(f) => {
                match schema.column(column_idx).data_type() {
                    kudu::DataType::Float => row.set(column_idx, f as f32),
                    _ => try!(row.set(column_idx, f)),
                }
            },
            &Literal::Timestamp(t) => {
                let epoch = chrono::NaiveDateTime::from_timestamp(0, 0);
                //let seconds = t.timestamp(),

                //match schema.column(column_idx).data_type() {
                    //kudu::DataType::Float => row.set(column_idx, f as f32),
                    //_ => row.set(column_idx, f),
                //}
                unimplemented!()

            },
            &Literal::String(s) => {
                unimplemented!()
            },
            &Literal::Binary(b) => {
                unimplemented!()
            },
        };
    }
    Ok(row)
}
*/

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
    seed: Option<i32>,
    buckets: i32,
}

impl <'a> HashPartition<'a> {
    pub fn new(columns: Vec<&'a str>, seed: Option<i32>, buckets: i32) -> HashPartition<'a> {
        HashPartition { columns: columns, seed: seed, buckets: buckets }
    }
}

#[derive(Debug, PartialEq)]
pub enum Literal<'a> {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    String(Cow<'a, str>),
    Binary(Vec<u8>),
}
