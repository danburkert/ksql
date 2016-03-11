use kudu;

use terminal::Terminal;

/// A command parsed from user input. The root of the SQL AST.
#[derive(Debug, PartialEq, Eq)]
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
            Command::CreateTable { .. } => (),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CreateColumn<'a> {
    name: &'a str,
    data_type: kudu::DataType,
    encoding_type: Option<kudu::EncodingType>,
    compression_type: Option<kudu::CompressionType>,
    block_size: Option<i32>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RangePartition<'a> {
    columns: Vec<&'a str>,
    split_rows: Vec<&'a str>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashPartition<'a> {
    columns: Vec<&'a str>,
    buckets: i32,
    seed: Option<i32>,
}

impl <'a> CreateColumn<'a> {
    pub fn new(name: &'a str,
               data_type: kudu::DataType,
               encoding_type: Option<kudu::EncodingType>,
               compression_type: Option<kudu::CompressionType>,
               block_size: Option<i32>)
               -> CreateColumn<'a> {
        CreateColumn {
            name: name,
            data_type: data_type,
            encoding_type: encoding_type,
            compression_type: compression_type,
            block_size: block_size,
        }
    }
}
