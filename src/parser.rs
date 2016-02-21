//! SQL Parsers
//!
//! This module defines parsers for kudusql's SQL dialect. The parsers are
//! written in the parser combinator style, and are heavily influenced by Nom
//! (but with fewer macros). Nom wasn't appropriate because it does not have the
//! ability to return a hint as the result of an incomplete parse.

#![allow(non_snake_case)]

use command;

/// Returns the result of a parse if not successful, otherwise returns the value
/// and remaining input.
macro_rules! try_parse {
    ($e:expr) => (match $e {
        $crate::parser::ParseResult::Ok(t, remaining) => (t, remaining),
        $crate::parser::ParseResult::Incomplete(hint, remaining) =>
            return $crate::parser::ParseResult::Incomplete(hint, remaining),
        $crate::parser::ParseResult::Err(err, remaining) =>
            return $crate::parser::ParseResult::Err(err, remaining),
    });
}

/// A hint which indicates what additional input is necessary to achieve a
/// successful parse.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Hint<'a> {

    /// Hint with a constant.
    Constant(&'a str),

    /// Hint with a table name. The prefix of the table name is included.
    Table(&'a str),
}

/// The result of a parse. The result is either `Ok` if the parse succeeds, or a
/// failure. The failure may either be `Incomplete`, which indicates that not
/// enough input was provided, along with a hint, or `Err` which indicates a
/// parsing error occurred.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ParseResult<'a, T> {
    /// A succesful parse including the parsed value, and the remaining unparsed input.
    Ok(T, &'a str),

    /// An incomplete parse including a hint, and the remaining unparsed input.
    Incomplete(Hint<'a>, &'a str),

    /// A parse error, including the expected input, and the remaining unparsed input.
    Err(&'a str, &'a str),
}

/// A standard interface for parsers.
///
/// Provides parser combinators.
///
/// TODO: the lifetime param shouldn't be necessary, but I can't figure out
/// another way to have the Output type be a &str.
trait Parser<'a> {
    type Output;
    fn parse(&self, &'a str) -> ParseResult<'a, Self::Output>;

    /// Returns a new parser which transforms the result of this parser using
    /// the supplied function.
    fn map<T, F>(self, f: F) -> Map<Self, F>
    where Self: Sized,
          F: Fn(Self::Output) -> T {
        Map(self, f)
    }

    /// Returns a new parser which evaluates this parser, and if it succeeds,
    /// returns the result of evaluating the provided parser.
    fn and_then<T, P>(self, p: P) -> AndThen<Self, P> where Self: Sized, P: Parser<'a, Output=T> {
        AndThen(self, p)
    }

    /// Returns a new parser which evaluates this parser, and if it succeeds,
    /// evaluates the provided parser. If the second parse succeeds, the
    /// original value is returned.
    fn followed_by<P>(self, p: P) -> FollowedBy<Self, P> where Self: Sized, P: Parser<'a> {
        FollowedBy(self, p)
    }

    /// Returns the result of evaluating this parser if successful, otherwise
    /// the result of the second parser if successful. If both are unsuccesful,
    /// then the incomplete parse that made the most progress is returned,
    /// or if both results are an error, the error result that made the most
    /// progress.
    fn or_else<P>(self, p: P) -> OrElse<Self, P>
    where Self: Sized,
          P: Parser<'a, Output=Self::Output> {
        OrElse(self, p)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Parser Combinators
////////////////////////////////////////////////////////////////////////////////

/// Applies the parser 0 or more times and returns the list of results in a Vec.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Many0<P>(P);
impl <'a, T, P> Parser<'a> for Many0<P> where P: Parser<'a, Output=T> {
    type Output = Vec<T>;
    fn parse(&self, mut input: &'a str) -> ParseResult<'a, Vec<T>> {
        let mut output = Vec::new();
        while let ParseResult::Ok(t, i) = self.0.parse(input) {
            assert!(i.len() < input.len());
            input = i;
            output.push(t);
        }
        ParseResult::Ok(output, input)
    }
}

/// Applies the parser 1 or more times and returns the list of results in a Vec.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Many1<P>(P);
impl <'a, T, P> Parser<'a> for Many1<P> where P: Parser<'a, Output=T> {
    type Output = Vec<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<T>> {
        let (t, mut remaining) = try_parse!(self.0.parse(input));
        let mut output = vec![t];
        while let ParseResult::Ok(t, r) = self.0.parse(remaining) {
            assert!(r.len() < remaining.len());
            remaining = r;
            output.push(t);
        }
        ParseResult::Ok(output, remaining)
    }
}

/// Applies the parser 0 or more times and returns nothing.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Ignore0<P>(P);
impl <'a, P> Parser<'a> for Ignore0<P> where P: Parser<'a> {
    type Output = ();
    fn parse(&self, mut input: &'a str) -> ParseResult<'a, ()> {
        while let ParseResult::Ok(_, i) = self.0.parse(input) {
            assert!(i.len() < input.len());
            input = i;
        }
        ParseResult::Ok((), input)
    }
}

/// Applies the parser 1 or more times and returns the list of results in a Vec.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Ignore1<P>(P);
impl <'a, T, P> Parser<'a> for Ignore1<P> where P: Parser<'a, Output=T> {
    type Output = ();
    fn parse(&self, input: &'a str) -> ParseResult<'a, ()> {
        let (_, mut remaining) = try_parse!(self.0.parse(input));
        while let ParseResult::Ok(_, r) = self.0.parse(remaining) {
            assert!(r.len() < remaining.len());
            remaining = r;
        }
        ParseResult::Ok((), remaining)
    }
}

pub struct OrElse<P1, P2>(P1, P2);
impl <'a, T, P1, P2> Parser<'a> for OrElse<P1, P2>
where P1: Parser<'a, Output=T>,
      P2: Parser<'a, Output=T> {
    type Output = T;
    fn parse(&self, input: &'a str) -> ParseResult<'a, T> {
        match self.0.parse(input) {
            ParseResult::Ok(t, remaining) => {
                ParseResult::Ok(t, remaining)
            },
            ParseResult::Incomplete(hint1, remaining1) => {
                match self.1.parse(input) {
                    ParseResult::Ok(t, remaining) => {
                        ParseResult::Ok(t, remaining)
                    },
                    ParseResult::Incomplete(hint2, remaining2) => {
                        // Both parses were incomplete. Return the incomplete
                        // result for the parse which made the most progress.
                        if remaining1.len() <= remaining2.len() {
                            ParseResult::Incomplete(hint1, remaining1)
                        } else {
                            ParseResult::Incomplete(hint2, remaining2)
                        }
                    },
                    ParseResult::Err(..) => {
                        ParseResult::Incomplete(hint1, remaining1)
                    },
                }
            },
            ParseResult::Err(error1, remaining1) => {
                match self.1.parse(input) {
                    ParseResult::Ok(t, remaining) => {
                        ParseResult::Ok(t, remaining)
                    },
                    ParseResult::Incomplete(hint2, remaining2) => {
                        ParseResult::Incomplete(hint2, remaining2)
                    },
                    ParseResult::Err(error2, remaining2) => {
                        // Both parses errored. Return the error result for the
                        // parse which made the most progress.
                        if remaining1.len() <= remaining2.len() {
                            ParseResult::Err(error1, remaining1)
                        } else {
                            ParseResult::Err(error2, remaining2)
                        }
                    },
                }
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct AndThen<P1, P2>(P1, P2);
impl <'a, T, P1, P2> Parser<'a> for AndThen<P1, P2>
where P1: Parser<'a>,
      P2: Parser<'a, Output=T> {
    type Output = T;
    fn parse(&self, input: &'a str) -> ParseResult<'a, T> {
        let (_, remaining) = try_parse!(self.0.parse(input));
        let (t, remaining) = try_parse!(self.1.parse(remaining));
        ParseResult::Ok(t, remaining)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct FollowedBy<P1, P2>(P1, P2);
impl <'a, T, P1, P2> Parser<'a> for FollowedBy<P1, P2>
where P1: Parser<'a, Output=T>,
      P2: Parser<'a> {
    type Output = T;
    fn parse(&self, input: &'a str) -> ParseResult<'a, T> {
        let (t, remaining) = try_parse!(self.0.parse(input));
        let (_, remaining) = try_parse!(self.1.parse(remaining));
        ParseResult::Ok(t, remaining)
    }
}

/// Returns the longest string until the provided function fails.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct TakeWhile0<F>(F);
impl <'a, F> Parser<'a> for TakeWhile0<F> where F: Fn(char) -> bool {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        for (idx, c) in input.char_indices() {
            if !self.0(c) {
                let (parsed, remaining) = input.split_at(idx);
                return ParseResult::Ok(parsed, remaining);
            }
        }
        ParseResult::Ok(input, "")
    }
}

/// Returns the longest (non-empty) string until the provided function fails.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct TakeWhile1<F>(F, &'static str);
impl <'a, F> Parser<'a> for TakeWhile1<F> where F: Fn(char) -> bool {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        let mut char_indices = input.char_indices();

        match char_indices.next() {
            None => return ParseResult::Incomplete(Hint::Constant(self.1), input),
            Some((_, c)) if !self.0(c) => return ParseResult::Err(self.1, input),
            _ => (),
        }

        for (idx, c) in char_indices {
            if !self.0(c) {
                let (parsed, remaining) = input.split_at(idx);
                return ParseResult::Ok(parsed, remaining);
            }
        }
        ParseResult::Ok(input, "")
    }
}

/// Transforms the result of a parser.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Map<P, F>(P, F);
impl <'a, T, U, P, F> Parser<'a> for Map<P, F>
where P: Parser<'a, Output=T>,
      F: Fn(T) -> U,
{
    type Output = U;
    fn parse(&self, input: &'a str) -> ParseResult<'a, U> {
        let (v, remaining) = try_parse!(self.0.parse(input));
        ParseResult::Ok(self.1(v), remaining)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Utility Parsers
////////////////////////////////////////////////////////////////////////////////

/// Parses the provided tag from the input.
pub struct Tag(&'static str);
impl <'a> Parser<'a> for Tag {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        use ::std::cmp::min;

        let tag_bytes = self.0.as_bytes();
        let tag_len = tag_bytes.len();

        let input_bytes = input.as_bytes();
        let input_len = input_bytes.len();

        let len = min(tag_len, input_len);

        if &tag_bytes[0..len] != &input_bytes[0..len] {
            ParseResult::Err(self.0, input)
        } else if len == tag_len {
            let (parsed, remaining) = input.split_at(len);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(Hint::Constant(self.0), input)
        }
    }
}

pub struct Char(char, &'static str);
impl <'a> Parser<'a> for Char {
    type Output = char;
    fn parse(&self, input: &'a str) -> ParseResult<'a, char> {
        match input.chars().next() {
            None => ParseResult::Incomplete(Hint::Constant(self.1), input),
            Some(c) if c == self.0 => ParseResult::Ok(self.0, &input[1..]),
            _ => ParseResult::Err(self.1, input),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SQL Parsers
////////////////////////////////////////////////////////////////////////////////

/// Parses the provided keyword from the input. The keyword should be only ASCII
/// capital letters, the parse is case-insensitive.
pub struct Keyword(&'static str);
impl <'a> Parser<'a> for Keyword {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        use ::std::cmp::min;
        use ::std::ascii::AsciiExt;

        let keyword_bytes = self.0.as_bytes();
        let keyword_len = keyword_bytes.len();

        let input_bytes = input.as_bytes();
        let input_len = input_bytes.len();

        let len = min(keyword_len, input_len);

        if &keyword_bytes[0..len] != &input_bytes[0..len].to_ascii_uppercase()[..] {
            ParseResult::Err(self.0, input)
        } else if len == keyword_len {
            let (parsed, remaining) = input.split_at(len);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(Hint::Constant(self.0), input)
        }
    }
}

/// Parses a SQL block comment:
///
/// ```sql
/// /* this is
///    a block comment */
/// ```
pub struct BlockComment;
impl <'a> Parser<'a> for BlockComment {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        let (_, remaining) = try_parse!(Tag("/*").parse(input));
        if let Some(idx) = remaining.find("*/") {
            let (parsed, remaining) = input.split_at(idx + 4);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(Hint::Constant("*/"), remaining)
        }
    }
}

/// Parses a SQL line comment:
///
/// ```sql
/// -- line comment
/// ```
pub struct LineComment;
impl <'a> Parser<'a> for LineComment {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        let (_, remaining) = try_parse!(Tag("--").parse(input));
        if let Some(idx) = remaining.find('\n') {
            let (parsed, remaining) = input.split_at(idx + 3);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Ok(input, "")
        }
    }
}

/// Returns true if the character is a multispace character.
pub fn is_multispace(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

/// Parses a SQL token delimiter. The delimiter may be a combination of
/// whitespace or a comment.
pub struct TokenDelimiter;
impl <'a> Parser<'a> for TokenDelimiter {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        TakeWhile1(is_multispace, " ")
            .or_else(LineComment)
            .or_else(BlockComment)
            .parse(input)
    }
}

pub struct ShowTables;
impl <'a> Parser<'a> for ShowTables {
    type Output = command::ShowTables;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::ShowTables> {
        Many0(TokenDelimiter)
            .and_then(Keyword("SHOW"))
            .and_then(Many1(TokenDelimiter))
            .and_then(Keyword("TABLES"))
            .and_then(Many0(TokenDelimiter))
            .and_then(Char(';', ";"))
            .map(|_| command::ShowTables)
            .parse(input)
    }
}

fn is_alphabetic(c: char) -> bool {
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

fn is_numeric(c: char) -> bool {
    c >= '0' && c <= '9'
}

fn is_identifier_char(c: char) -> bool {
    is_alphabetic(c) || is_numeric(c) || c == '_'
}

/// Parses a table name. If the table name is suspected to be incomplete (the
/// parse ends with no more input), then an incompete result is returned with a
/// table hint.
pub struct TableName;
impl <'a> Parser<'a> for TableName {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        match TakeWhile1(is_identifier_char, "").parse(input) {
            ParseResult::Incomplete(_, remaining) =>
                ParseResult::Incomplete(Hint::Table(""), remaining),
            ParseResult::Ok(identifier, remaining) => {
                if remaining.is_empty() {
                    ParseResult::Incomplete(Hint::Table(identifier), input)
                } else {
                    ParseResult::Ok(identifier, remaining)
                }
            }
            ParseResult::Err(_, remaining) => ParseResult::Err("", remaining),
        }
    }
}

/// Parses a describe table statement.
pub struct DescribeTable;
impl <'a> Parser<'a> for DescribeTable {
    type Output = command::DescribeTable<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::DescribeTable<'a>> {
        Many0(TokenDelimiter)
            .and_then(Keyword("DESCRIBE"))
            .and_then(Ignore1(TokenDelimiter))
            .and_then(Keyword("TABLE"))
            .and_then(Ignore1(TokenDelimiter))
            .and_then(TableName).map(|table| command::DescribeTable { table: table })
            .followed_by(Ignore0(TokenDelimiter))
            .followed_by(Char(';', ";"))
            .parse(input)
    }
}

/// Parses a describe table statement.
pub struct Command;
impl <'a> Parser<'a> for Command {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        ShowTables.map(command::Command::ShowTables)
                  .or_else(DescribeTable.map(command::Command::DescribeTable))
                  .parse(input)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::Parser;
    use command;

    #[test]
    fn test_try_parse() {
        fn try_parse<T>(result: ParseResult<T>) -> ParseResult<T> {
            let (value, remaining) = try_parse!(result);
            ParseResult::Ok(value, remaining)
        }

        for result in vec![ParseResult::Ok(13, "fuzz"),
                           ParseResult::Incomplete(Hint::Constant("foo"), "fuzz"),
                           ParseResult::Err("SELECT", "fuzz")] {
            assert_eq!(result, try_parse(result));
        }
    }

    #[test]
    fn test_token_delimiter() {
        let parser = TokenDelimiter;
        assert_eq!(parser.parse("  "), ParseResult::Ok("  ", ""));
        assert_eq!(parser.parse(" a "), ParseResult::Ok(" ", "a "));

        assert_eq!(parser.parse(""), ParseResult::Incomplete(Hint::Constant(" "), ""));

        assert_eq!(parser.parse("S"), ParseResult::Err(" ", "S"));
    }

    #[test]
    fn test_keyword() {
        let parser = Keyword("FOO");
        assert_eq!(parser.parse("FOO"), ParseResult::Ok("FOO", ""));
        assert_eq!(parser.parse("foo"), ParseResult::Ok("foo", ""));
        assert_eq!(parser.parse("FoO"), ParseResult::Ok("FoO", ""));
        assert_eq!(parser.parse("foo bar baz"), ParseResult::Ok("foo", " bar baz"));

        assert_eq!(parser.parse("fo"), ParseResult::Incomplete(Hint::Constant("FOO"), "fo"));

        assert_eq!(parser.parse("fub"), ParseResult::Err("FOO", "fub"));
    }

    #[test]
    fn test_block_comment() {
        let parser = BlockComment;
        let multiline = "/* fooo bar
                          *
                          *
                          *
                         * /*\n/*\r/*\t/
                         / * / ** // //////// ***
                         baz \t\n\r\t
                         fizz */buzz";
        assert_eq!(parser.parse(multiline), ParseResult::Ok(&multiline[..multiline.len() - 4], "buzz"));
        assert_eq!(parser.parse("/* foo */"), ParseResult::Ok("/* foo */", ""));

        assert_eq!(parser.parse( "/* fuzz"),
                   ParseResult::Incomplete(Hint::Constant("*/"), " fuzz"));

        assert_eq!(parser.parse(""), ParseResult::Incomplete(Hint::Constant("/*"), ""));
        assert_eq!(parser.parse("/"), ParseResult::Incomplete(Hint::Constant("/*"), "/"));

        assert_eq!(parser.parse("*/"), ParseResult::Err("/*", "*/"));
        assert_eq!(parser.parse(" foo"), ParseResult::Err("/*", " foo"));
    }

    #[test]
    fn test_line_comment() {
        let parser = LineComment;
        let multiline = "-- foo bar \t\t\t \t fuzz /* \\ buster
SELECT";
        assert_eq!(parser.parse(multiline),
                   ParseResult::Ok(&multiline[..multiline.len() - 6], "SELECT"));
        assert_eq!(parser.parse("-- foo bar bazz\t\r\r\nfuzz"),
                   ParseResult::Ok("-- foo bar bazz\t\r\r\n", "fuzz"));

        assert_eq!(parser.parse("-"), ParseResult::Incomplete(Hint::Constant("--"), "-"));

        assert_eq!(parser.parse(" --"), ParseResult::Err("--", " --"));
        assert_eq!(parser.parse("- "), ParseResult::Err("--", "- "));
    }

    #[test]
    fn test_show_tables() {
        let parser = ShowTables;
        assert_eq!(parser.parse(" SHOW TABLES ;"), ParseResult::Ok(command::ShowTables, ""));
        assert_eq!(parser.parse("shOw TABLES;"), ParseResult::Ok(command::ShowTables, ""));
        assert_eq!(parser.parse("\n\n\tshOw \t\r\nTABLES;next command"),
                   ParseResult::Ok(command::ShowTables, "next command"));

        assert_eq!(parser.parse("SHOW--mycoment ; foo bar\nTABLES;"),
                   ParseResult::Ok(command::ShowTables, ""));
    }

    #[test]
    fn test_describe_table() {
        let parser = DescribeTable;
        assert_eq!(parser.parse("DESCRIBE TABLE _foo1_bAr2;"),
                   ParseResult::Ok(command::DescribeTable { table: "_foo1_bAr2" }, ""));

        assert_eq!(parser.parse("DESCRIBE TABLE \n\t\r -- some comment\n_______--another comment\n ; SELECT"),
                   ParseResult::Ok(command::DescribeTable { table: "_______" }, " SELECT"));

        assert_eq!(parser.parse("DESCRIBE TABLE -- t;\n"),
                   ParseResult::Incomplete(Hint::Table(""), ""));
        assert_eq!(parser.parse("DESCRIBE/* */TABLE/* foo */foo"),
                   ParseResult::Incomplete(Hint::Table("foo"), "foo"));

        assert_eq!(parser.parse("DESCRIBETABLE foo;"),
                   ParseResult::Err(" ", "TABLE foo;"));
        assert_eq!(parser.parse("DESCRIBE/**/TABLE foo."),
                   ParseResult::Err(";", "."));
    }

    #[test]
    fn test_command() {
        let parser = Command;
        assert_eq!(parser.parse("DESCRIBE TABLE _foo_bAr;"),
                   ParseResult::Ok(command::Command::DescribeTable(
                           command::DescribeTable { table: "_foo_bAr" }), ""));
        assert_eq!(parser.parse(" \n Show Tables;  SELECT"),
                   ParseResult::Ok(command::Command::ShowTables(command::ShowTables), "  SELECT"));

        assert_eq!(parser.parse("show t"),
                   ParseResult::Incomplete(Hint::Constant("TABLES"), "t"));
        assert_eq!(parser.parse("   describe t"),
                   ParseResult::Incomplete(Hint::Constant("TABLE"), "t"));

        assert_eq!(parser.parse("describe --\n foo"),
                   ParseResult::Err("TABLE", "foo"));
    }
}
