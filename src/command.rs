/// A command parsed from user input. The root of the SQL AST.
#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    ShowTables(ShowTables),
    DescribeTable(DescribeTable<'a>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShowTables;

#[derive(Debug, PartialEq, Eq)]
pub struct DescribeTable<'a> {
    pub table: &'a str,
}
