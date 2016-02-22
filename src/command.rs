use kudu;

/// A command parsed from user input. The root of the SQL AST.
#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    /// /* no-op */ ;
    Noop,

    /// SHOW TABLES;
    ShowTables,

    /// DESCRIBE TABLE <table>;
    DescribeTable {
        table: &'a str,
    },
}

impl <'a> Command<'a> {

    pub fn execute(self, client: &mut kudu::Client) {
        match self {
            Command::Noop => (),
            Command::ShowTables => {
                match client.list_tables() {
                    Ok(tables) => {
                        for table in tables {
                            println!("{}", table);
                        }
                    }
                    Err(error) => {
                        println!("{}", error);
                    }
                }
            },
            Command::DescribeTable { table } => {
                let schema = client.table_schema(table).unwrap();
                let num_columns = schema.num_columns();
                let num_primary_key_columns = schema.num_primary_key_columns();

                let mut primary_key_columns = Vec::new();

                for idx in 0..num_columns {
                    let column = schema.column(idx);
                    let name = column.name();
                    if idx < num_primary_key_columns {
                        primary_key_columns.push(name.to_string());
                    }
                    println!("{} {:?} {}",
                             name,
                             column.data_type(),
                             if column.is_nullable() { "NULLABLE" } else { "NOT NULL" });
                }
                println!("PRIMARY KEY({})", primary_key_columns.join(", "));
            },
        }
    }
}
