use kudu;

use terminal::Terminal;

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

    pub fn execute(self, client: &mut kudu::Client, term: &mut Terminal) {
        match self {
            Command::Noop => (),
            Command::ShowTables => {
                match client.list_tables() {
                    Ok(tables) => term.print_tables(&tables),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
            Command::DescribeTable { table } => {
                match client.table_schema(table) {
                    Ok(schema) => term.print_table(&schema),
                    Err(error) => term.print_kudu_error(&error),
                }
            },
        }
    }
}
