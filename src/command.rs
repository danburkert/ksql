#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    ShowTables(ShowTables),
    DescribeTable(DescribeTable),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShowTables;

#[derive(Debug, PartialEq, Eq)]
pub struct DescribeTable {
    pub table: String,
}
