use std::borrow::Cow;
use std::sync::mpsc::sync_channel;
use std::time::{
    Duration,
    Instant,
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

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

impl <'a> Command<'a> {

    pub fn execute(self, client: &kudu::Client, term: &mut Terminal) {
        match self {
            Command::Noop => (),
            Command::Help => term.print_help(),
            Command::ShowTables => match client.list_tables(deadline()) {
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
                let deadline = deadline();
                match client.open_table(table, deadline) {
                    Ok(table) => match table.list_tablets(deadline) {
                        Ok(tablets) => term.print_create_table(table, tablets),
                        Err(error) => term.print_kudu_error(&error),
                    },
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::ShowMasters => match client.list_masters(deadline()) {
                Ok(masters) => term.print_masters(masters),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::ShowTabletServers => match client.list_tablet_servers(deadline()) {
                Ok(tablet_servers) => term.print_tablet_servers(tablet_servers),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::ShowTableTablets { table } => {
                let deadline = deadline();
                match client.open_table(table, deadline)
                            .and_then(|table| table.list_tablets(deadline)) {
                    Ok(tablets) => term.print_tablets(tablets),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::ShowTableReplicas { table } => {
                let deadline = deadline();
                match client.open_table(table, deadline)
                            .and_then(|table| table.list_tablets(deadline)) {
                    Ok(tablets) => term.print_replicas(tablets),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::DescribeTable { table } => match client.open_table(table, deadline()) {
                Ok(table) => term.print_table_description(table.schema()),
                Err(error) => term.print_kudu_error(&error),
            },
            Command::DropTable { table } => {
                match client.delete_table(table, deadline()) {
                    Ok(_) => term.print_success("table dropped"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::Select { .. /*table, selector*/ } => {
                /*
                match select(client, table, selector) {
                    Ok(table) => term.print_table(&table[..]),
                    Err(error) => term.print_kudu_error(&error),
                }
                */
                term.print_not_implemented()
            },
            Command::Insert { table, columns, rows } => {
                match insert(client, table, columns, rows) {
                    Ok(stats) => term.print_success(&format!("{} rows inserted, {} rows failed",
                                                             stats.successful_operations(),
                                                             stats.failed_operations())),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::CreateTable { name, columns, primary_key, range_partition, hash_partitions, replicas } => {
                match create_table(client, name, columns, primary_key, range_partition, hash_partitions, replicas) {
                    Ok(_) => term.print_success("table created"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::AlterTable { table_name, steps } => {
                match alter_table(client, table_name, steps) {
                    Ok(_) => term.print_success("table altered"),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
        }
    }
}

fn create_table<'a>(client: &kudu::Client,
                    name: &str,
                    mut columns: Vec<ColumnSpec>,
                    primary_key: Vec<&str>,
                    range_partition: Option<RangePartition<'a>>,
                    hash_partitions: Vec<HashPartition>,
                    replicas: Option<u32>)
                    -> kudu::Result<()> {
    let mut schema = kudu::SchemaBuilder::new();

    for column_name in &primary_key {
        if let Some(mut column) = columns.iter_mut().find(|column| &column.name() == column_name) {
            column.not_null = true;
        }
    }

    for column in columns {
        schema.add_column_by_ref(column.into_column());
    }
    schema.set_primary_key_by_ref(primary_key);
    let schema = try!(schema.build());

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
            let (lower_bound, upper_bound) =
                try!(convert_bounds(&schema, &columns, lower_bound, upper_bound));
            table_builder.add_range_partition(lower_bound, upper_bound);
        }
    } else {
        table_builder.set_range_partition_columns(Vec::<String>::new());
    }

    for hash_partition in hash_partitions {
        table_builder.add_hash_partition_with_seed(hash_partition.columns,
                                                   hash_partition.buckets,
                                                   hash_partition.seed.unwrap_or(0));
    }

    if let Some(replicas) = replicas {
        table_builder.set_num_replicas(replicas);
    }

    let table_id = try!(client.create_table(table_builder, deadline()));
    client.wait_for_table_creation_by_id(&table_id, deadline())
}

fn alter_table(client: &kudu::Client,
               table_name: &str,
               steps: Vec<AlterTableStep>)
               -> kudu::Result<()> {
    let mut builder = kudu::AlterTableBuilder::new();
    let table = try!(client.open_table(table_name, deadline()));
    let mut columns = Vec::new();
    for &idx in table.partition_schema().range_partition_schema().columns() {
        columns.push((idx, table.schema().columns()[idx].data_type()));
    }

    for step in steps {
        match step {
            AlterTableStep::RenameTable { table_name } => {
                builder.rename_table_by_ref(table_name);
            },
            AlterTableStep::RenameColumn { old_column_name, new_column_name } => {
                builder.rename_column_by_ref(old_column_name, new_column_name);
            },
            AlterTableStep::AddColumn { column } => {
                builder.add_column_by_ref(column.into_column());
            },
            AlterTableStep::DropColumn { column_name } => {
                builder.drop_column_by_ref(column_name);
            },
            AlterTableStep::AddRangePartition(lower_bound, upper_bound) => {
                let (lower_bound, upper_bound) =
                    try!(convert_bounds(table.schema(), &columns, lower_bound, upper_bound));
                builder.add_range_partition_by_ref(&lower_bound, &upper_bound);
            },
            AlterTableStep::DropRangePartition(lower_bound, upper_bound) => {
                let (lower_bound, upper_bound) =
                    try!(convert_bounds(table.schema(), &columns, lower_bound, upper_bound));
                builder.drop_range_partition_by_ref(&lower_bound, &upper_bound);
            },
        }
    }

    let table_id = try!(client.alter_table(table_name, builder, deadline()));
    client.wait_for_table_alteration_by_id(&table_id, deadline())
}

fn convert_bounds<'a>(schema: &kudu::Schema,
                      columns: &[(usize, kudu::DataType)],
                      lower_bound: Bound<'a>,
                      upper_bound: Bound<'a>)
                      -> kudu::Result<(kudu::RangePartitionBound, kudu::RangePartitionBound)> {
    let mut lower = schema.new_row();
    let lower_bound = match lower_bound {
        Bound::Inclusive(values) => {
            try!(populate_row(&mut lower, &columns, &values));
            kudu::RangePartitionBound::Inclusive(lower)
        },
        Bound::Exclusive(values) => {
            try!(populate_row(&mut lower, &columns, &values));
            kudu::RangePartitionBound::Exclusive(lower)
        },
        Bound::Unbounded => kudu::RangePartitionBound::Exclusive(lower),
    };
    let mut upper = schema.new_row();
    let upper_bound = match upper_bound {
        Bound::Inclusive(values) => {
            try!(populate_row(&mut upper, &columns, &values));
            kudu::RangePartitionBound::Inclusive(upper)
        },
        Bound::Exclusive(values) => {
            try!(populate_row(&mut upper, &columns, &values));
            kudu::RangePartitionBound::Exclusive(upper)
        },
        Bound::Unbounded => kudu::RangePartitionBound::Exclusive(upper),
    };
    Ok((lower_bound, upper_bound))
}

/*
fn select(client: &kudu::Client,
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

fn scan(client: &kudu::Client,
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

fn count(client: &kudu::Client, table: &str) -> kudu::Result<Vec<Vec<String>>> {
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
*/

fn insert(client: &kudu::Client,
          table: &str,
          columns: Option<Vec<&str>>,
          rows: Vec<Vec<Literal>>)
          -> kudu::Result<kudu::FlushStats> {
    let table = try!(client.open_table(table, deadline()));

    let writer = table.new_writer(kudu::WriterConfig::default());

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

    for row in rows {
        let mut r = table.schema().new_row();

        try!(populate_row(&mut r, &column_types, &row));
        writer.insert(r);
    }

    let (send, recv) = sync_channel(100);
    writer.flush(move |stats| send.send(stats).unwrap());
    Ok(recv.recv().unwrap())
}

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
                try!(row.set(column_idx, time))
            },
            (kudu::DataType::Float, &Literal::Float(f)) => try!(row.set(column_idx, f as f32)),
            (_, &Literal::Float(f)) => try!(row.set(column_idx, f)),
            (_, &Literal::String(ref s)) => try!(row.set(column_idx, &s[..])),
            (_, &Literal::Binary(ref b)) => try!(row.set(column_idx, &b[..])),
            (_, &Literal::Null) => try!(row.set_null(column_idx)),
        };
    };
    Ok(())
}

/*
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
*/

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
            column.set_not_null_by_ref();
        }
        if let Some(encoding_type) = self.encoding_type {
            column.set_encoding_by_ref(encoding_type);
        }
        if let Some(compression_type) = self.compression_type {
            column.set_compression_by_ref(compression_type);
        }
        if let Some(block_size) = self.block_size {
            column.set_block_size_by_ref(block_size);
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
