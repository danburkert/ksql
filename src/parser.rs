use std::str;

use nom;

use command::{
    Command,
    DescribeTable,
    ShowTables,
};

macro_rules! keyword(
    ($input:expr, $keyword:expr) => ({
        use ::std::cmp::min;
        use ::std::ascii::AsciiExt;
        #[inline(always)]
        fn as_bytes<T: ::nom::AsBytes>(b: &T) -> &[u8] {
            b.as_bytes()
        }

        let expected = $keyword;
        let bytes = as_bytes(&expected);

        let len = $input.len();
        let blen = $keyword.len();
        let m = min(len, blen);
        let reduced = &$input[..m];
        let b       = &bytes[..m];

        if reduced.to_ascii_uppercase() != b {
            ::nom::IResult::Error(::nom::Err::Position(::nom::ErrorKind::Tag, $input))
        } else if m < blen {
            ::nom::IResult::Incomplete(::nom::Needed::Size(blen))
        } else {
            ::nom::IResult::Done(&$input[blen..], reduced)
        }
    });
);

pub fn is_identifier_char(c: u8) -> bool {
    nom::is_alphanumeric(c) || c == '_' as u8 || c == '-' as u8
}

pub fn is_multispace(c: u8) -> bool {
    c == ' ' as u8 || c == '\t' as u8 || c == '\r' as u8 || c == '\n' as u8
}

named!(multispace0, take_while!(is_multispace));
named!(multispace1, take_while1!(is_multispace));

// TODO: allow escaped identifiers
named!(identifier<&str>, map_res!(take_while1!(is_identifier_char), str::from_utf8));

named!(pub show_tables<ShowTables>,
    chain!(
        multispace0             ~
        keyword!("SHOW")        ~
        multispace1             ~
        keyword!("TABLES")      ~
        multispace0             ~
        char!(';')              ,
        || { ShowTables }));

named!(pub describe_table<DescribeTable>,
    chain!(
       multispace0              ~
       keyword!("DESCRIBE")     ~
       multispace1              ~
       keyword!("TABLE")        ~
       multispace1              ~
       table: identifier        ~
       multispace0              ~
       char!(';')               ,
       || { DescribeTable { table: table.to_string() } }));

named!(pub command<Command>, alt!(
    show_tables => { |c| Command::ShowTables(c) }
  | describe_table => { |c| Command::DescribeTable(c) }));

#[cfg(test)]
mod tests {
    use super::*;

    use command::{
        Command,
        DescribeTable,
        ShowTables,
    };

    use nom::{
        Err,
        ErrorKind,
        IResult,
        Needed,
    };

    #[test]
    fn test_keyword() {
        named!(select, keyword!("SELECT"));

        assert_eq!(select(b"SELECT"), IResult::Done(&b""[..], &b"SELECT"[..]));
        assert_eq!(select(b"SELECT "), IResult::Done(&b" "[..], &b"SELECT"[..]));
        assert_eq!(select(b"SELECT foo"), IResult::Done(&b" foo"[..], &b"SELECT"[..]));
        assert_eq!(select(b"SELECT_"), IResult::Done(&b"_"[..], &b"SELECT"[..]));

        assert_eq!(select(b"select"), IResult::Done(&b""[..], &b"select"[..]));
        assert_eq!(select(b"sEleCt"), IResult::Done(&b""[..], &b"sEleCt"[..]));
        assert_eq!(select(b"SElEcT foo"), IResult::Done(&b" foo"[..], &b"SElEcT"[..]));

        assert_eq!(select(b"fuzz"), IResult::Error(Err::Position(ErrorKind::Tag, &b"fuzz"[..])));
        assert_eq!(select(b"SELEC t"), IResult::Error(Err::Position(ErrorKind::Tag, &b"SELEC t"[..])));
        assert_eq!(select(b" SELECT"), IResult::Error(Err::Position(ErrorKind::Tag, &b" SELECT"[..])));

        assert_eq!(select(b""), IResult::Incomplete(Needed::Size(6)));
        assert_eq!(select(b"SELEC"), IResult::Incomplete(Needed::Size(6)));
        assert_eq!(select(b"s"), IResult::Incomplete(Needed::Size(6)));
    }

    #[test]
    fn test_show_tables() {
        assert_eq!(show_tables(b"SHOW TABLES;"), IResult::Done(&b""[..], ShowTables));
        assert_eq!(show_tables(b"shOw TABLES;"), IResult::Done(&b""[..], ShowTables));
        assert_eq!(show_tables(b"\n\n\tshOw \t\r\nTABLES;next command"),
                   IResult::Done(&b"next command"[..], ShowTables));
    }

    #[test]
    fn test_describe_table() {
        assert_eq!(describe_table(b"DESCRIBE TABLE _foo-bAr;"),
                   IResult::Done(&b""[..], DescribeTable { table: "_foo-bAr".to_string() }));
    }

    #[test]
    fn test_command() {
        assert_eq!(command(b"DESCRIBE TABLE _foo-bAr;"),
                   IResult::Done(&b""[..], Command::DescribeTable(
                           DescribeTable { table: "_foo-bAr".to_string() })));

        assert_eq!(command(b" \n Show Tables;  "),
                   IResult::Done(&b"  "[..], Command::ShowTables(ShowTables)));
    }
}
