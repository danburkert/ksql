//! SQL Parsers
//!
//! This module defines parsers for kudusql's SQL dialect. The parsers are
//! written in the parser combinator style, and are heavily influenced by Nom
//! (but with fewer macros). Nom wasn't appropriate because it does not have the
//! ability to return a hint as the result of an incomplete parse.

use std::borrow::Cow;
use std::cmp;

use kudu;

use command;

/// A hint which indicates what additional input is necessary to achieve a
/// successful parse.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Hint<'a> {
    /// Hint with a constant.
    Constant(&'static str),

    /// Hint with an integer value.
    Integer,

    /// Hint with a column value.
    Value,

    /// Hint with a character escape sequence.
    CharEscape,

    /// Hint with a hex escape sequence.
    HexEscape,

    /// Hint with a table name. The prefix of the table name is included.
    Table(&'a str),

    /// Hint with a column name. The prefix of the column name is included.
    Column(&'a str),
}

/// The result of a parse. The result is either `Ok` if the parse succeeds, or a
/// failure. The failure may either be `Incomplete`, which indicates that not
/// enough input was provided, along with a hint, or `Err` which indicates a
/// parsing error occurred.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ParseResult<'a, T> {
    /// A succesful parse including the parsed value, and the remaining unparsed input.
    Ok(T, &'a str),

    /// An incomplete parse including a hint and the remaining unparsed input
    /// for each parse attempt.
    Incomplete(Vec<(Hint<'a>, &'a str)>),

    /// A parse error, including the expected input as hints, and the remaining
    /// unparsed input.
    Err(Vec<Hint<'a>>, &'a str),
}

/// A standard interface for parsers.
///
/// Provides parser combinators.
///
/// TODO: the lifetime param shouldn't be necessary, but I can't figure out
/// another way to have the Output type be a &str.
pub trait Parser<'a> {
    type Output;
    fn parse(&self, &'a str) -> ParseResult<'a, Self::Output>;

    /// Returns a new parser which transforms the result of this parser using
    /// the supplied function.
    fn map<T, F>(self, f: F) -> Map<Self, F>
    where Self: Sized,
          F: Fn(Self::Output) -> T {
        Map(self, f)
    }

    fn map_incomplete<F>(self, f: F) -> MapIncomplete<Self, F>
    where Self: Sized,
          F: Fn(Vec<(Hint<'a>, &'a str)>) -> Vec<(Hint<'a>, &'a str)> {
        MapIncomplete(self, f)
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

/// Applies the parser until the input is empty, returning the vector of results.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
struct Many<P>(P);
impl <'a, T, P> Parser<'a> for Many<P> where P: Parser<'a, Output=T> {
    type Output = Vec<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<T>> {
        let (t, mut remaining) = try_parse!(self.0.parse(input));
        let mut output = vec![t];
        while !remaining.is_empty() {
            let (t, r) = try_parse!(self.0.parse(remaining));
            remaining = r;
            output.push(t);
        }
        ParseResult::Ok(output, remaining)
    }
}

/// Applies the parser at least once, with the provided delimiter.
struct Delimited1<P, D>(P, D);
impl <'a, T, P, D> Parser<'a> for Delimited1<P, D>
where P: Parser<'a, Output=T>,
      D: Parser<'a> {
    type Output = Vec<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<T>> {
        let (t, mut remaining) = try_parse!(self.0.parse(input));
        let mut output = vec![t];
        loop {
            if let ParseResult::Ok(_, r) = self.1.parse(remaining) {
                if let ParseResult::Ok(t, r) = self.0.parse(r) {
                    remaining = r;
                    output.push(t);
                } else { break; }
            } else { break; }
        }
        ParseResult::Ok(output, remaining)
    }
}

/// Applies the parser 0 or more times and returns nothing.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
struct Ignore0<P>(P);
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

/// Applies the parser 1 or more times and ignores the results.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
struct Ignore1<P>(P);
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
            ParseResult::Incomplete(mut hints1) => {
                match self.1.parse(input) {
                    ParseResult::Ok(t, remaining) => {
                        ParseResult::Ok(t, remaining)
                    },
                    ParseResult::Incomplete(hints2) => {
                        hints1.extend_from_slice(&hints2);
                        ParseResult::Incomplete(hints1)
                    },
                    ParseResult::Err(..) => {
                        ParseResult::Incomplete(hints1)
                    },
                }
            },
            ParseResult::Err(error1, remaining1) => {
                match self.1.parse(input) {
                    ParseResult::Ok(t, remaining) => ParseResult::Ok(t, remaining),
                    ParseResult::Incomplete(hints2) => ParseResult::Incomplete(hints2),
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
pub struct TakeWhile1<F>(F, Hint<'static>);
impl <'a, F> Parser<'a> for TakeWhile1<F> where F: Fn(char) -> bool {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        let mut char_indices = input.char_indices();

        match char_indices.next() {
            None => return ParseResult::Incomplete(vec![(self.1, input)]),
            Some((_, c)) if !self.0(c) => return ParseResult::Err(vec![self.1], input),
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

/// Transforms the incomplete result of a parser.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct MapIncomplete<P, F>(P, F);
impl <'a, T, P, F> Parser<'a> for MapIncomplete<P, F>
where P: Parser<'a, Output=T>,
      F: Fn(Vec<(Hint<'a>, &'a str)>) -> Vec<(Hint<'a>, &'a str)>,
{
    type Output = T;
    fn parse(&self, input: &'a str) -> ParseResult<'a, T> {

        match self.0.parse(input) {
            ParseResult::Ok(t, rest) => ParseResult::Ok(t, rest),
            ParseResult::Incomplete(hints) => ParseResult::Incomplete(self.1(hints)),
            ParseResult::Err(hints, rest) => ParseResult::Err(hints, rest),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct Optional<P>(P);
impl <'a, P, T> Parser<'a> for Optional<P>
where P: Parser<'a, Output=T> {
    type Output = Option<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Option<T>> {
        if let ParseResult::Ok(v, remaining) = self.0.parse(input) {
            ParseResult::Ok(Some(v), remaining)
        } else {
            ParseResult::Ok(None, input)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Utility Parsers
////////////////////////////////////////////////////////////////////////////////

/// Parses the provided tag from the input.
struct Tag(&'static str);
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
            ParseResult::Err(vec![Hint::Constant(self.0)], input)
        } else if len == tag_len {
            let (parsed, remaining) = input.split_at(len);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(vec![(Hint::Constant(self.0), input)])
        }
    }
}

struct Char(char, &'static str);
impl <'a> Parser<'a> for Char {
    type Output = char;
    fn parse(&self, input: &'a str) -> ParseResult<'a, char> {
        match input.chars().next() {
            None => ParseResult::Incomplete(vec![(Hint::Constant(self.1), input)]),
            Some(c) if c == self.0 => ParseResult::Ok(self.0, &input[1..]),
            _ => ParseResult::Err(vec![Hint::Constant(self.1)], input),
        }
    }
}

struct I32;
impl <'a> Parser<'a> for I32 {
    type Output = i32;
    fn parse(&self, input: &'a str) -> ParseResult<'a, i32> {
        if input.is_empty() {
            return ParseResult::Incomplete(vec![(Hint::Integer, input)]);
        }

        let mut idx = 0;
        for &c in input.as_bytes() {
            if c < '0' as u8 || c > '9' as u8 {
                break;
            } else {
                idx += 1;
            }
        }

        let (int, rest) = input.split_at(idx);
        match int.parse::<i32>() {
            Ok(int) => ParseResult::Ok(int, rest),
            Err(error) => ParseResult::Err(vec![Hint::Integer], input),
        }
    }
}

/// Parses the provided keyword from the input. The keyword should be only ASCII
/// capital letters, the parse is case-insensitive.
struct Keyword(&'static str);
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

        let upcase = input_bytes.to_ascii_uppercase();

        for i in 0..len {
            if keyword_bytes[i] != upcase[i] {
                return ParseResult::Err(vec![Hint::Constant(&self.0[i..])], &input[i..]);
            }
        }

        if len == keyword_len {
            let (parsed, remaining) = input.split_at(len);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(vec![(Hint::Constant(&self.0[len..]), &input[len..])])
        }
    }
}

/// Parses a SQL block comment:
///
/// ```sql
/// /* this is
///    a block comment */
/// ```
struct BlockComment;
impl <'a> Parser<'a> for BlockComment {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        let (_, remaining) = try_parse!(Tag("/*").parse(input));
        if let Some(idx) = remaining.find("*/") {
            let (parsed, remaining) = input.split_at(idx + 4);
            ParseResult::Ok(parsed, remaining)
        } else {
            ParseResult::Incomplete(vec![(Hint::Constant("*/"), remaining)])
        }
    }
}

/// Parses a SQL line comment:
///
/// ```sql
/// -- line comment
/// ```
struct LineComment;
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
fn is_multispace(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

/// Parses a SQL token delimiter. The delimiter may be a combination of
/// whitespace or a comment.
struct TokenDelimiter;
impl <'a> Parser<'a> for TokenDelimiter {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        TakeWhile1(is_multispace, Hint::Constant(" "))
            .or_else(LineComment)
            .or_else(BlockComment)
            .map_incomplete(|_| vec![(Hint::Constant(" "), input)])
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
struct TableName;
impl <'a> Parser<'a> for TableName {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        match TakeWhile1(is_identifier_char, Hint::Table("")).parse(input) {
            ParseResult::Ok(identifier, remaining) => {
                if remaining.is_empty() {
                    ParseResult::Incomplete(vec![(Hint::Table(identifier), input)])
                } else {
                    ParseResult::Ok(identifier, remaining)
                }
            }
            ParseResult::Incomplete(hints) => ParseResult::Incomplete(hints),
            ParseResult::Err(hints, remaining) => ParseResult::Err(hints, remaining),
        }
    }
}

/// Parses a column name. If the column name is suspected to be incomplete (the
/// parse ends with no more input), then an incompete result is returned with a
/// column hint.
struct ColumnName;
impl <'a> Parser<'a> for ColumnName {
    type Output = &'a str;
    fn parse(&self, input: &'a str) -> ParseResult<'a, &'a str> {
        match TakeWhile1(is_identifier_char, Hint::Column("")).parse(input) {
            ParseResult::Ok(identifier, remaining) => {
                if remaining.is_empty() {
                    ParseResult::Incomplete(vec![(Hint::Column(identifier), input)])
                } else {
                    ParseResult::Ok(identifier, remaining)
                }
            }
            ParseResult::Incomplete(hints) => ParseResult::Incomplete(hints),
            ParseResult::Err(hints, remaining) => ParseResult::Err(hints, remaining),
        }
    }
}

struct DoubleQuotedStringLiteral;
impl <'a> Parser<'a> for DoubleQuotedStringLiteral {
    type Output = Cow<'a, str>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Cow<'a, str>> {
        if input.is_empty() { return ParseResult::Incomplete(vec![(Hint::Constant("\""), input)]) };
        if &input[0..1] != "\"" { return ParseResult::Err(vec![Hint::Constant("\"")], input); }

        let mut escape_idx = None;
        for (idx, c) in input[1..].char_indices() {
            let idx = idx + 1; // offset for leading double quote
            match c {
                '\"' => return ParseResult::Ok(Cow::Borrowed(&input[1..idx]), &input[idx+1..]),
                '\\' => { escape_idx = Some(idx); break; },
                _ => (),
            }
        }

        if let Some(escape_idx) = escape_idx {
            let mut value = String::new();
            value.push_str(&input[1..escape_idx]);
            let mut escaped = true;
            for (idx, c) in input[escape_idx+1..].char_indices() {
                let idx = idx + escape_idx;
                if escaped {
                    match c {
                        '0' => value.push('\0'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        'n' => value.push('\n'),
                        '\'' => value.push('\''),
                        '\"' => value.push('\"'),
                        '\\' => value.push('\\'),
                        _ => return ParseResult::Err(vec![Hint::CharEscape], &input[idx+1..]),
                    }
                    escaped = false;
                } else {
                    match c {
                        '\\' => escaped = true,
                        '\"' => return ParseResult::Ok(Cow::Owned(value), &input[idx+2..]),
                        c => value.push(c),
                    }
                }
            }
        }
        ParseResult::Incomplete(vec![(Hint::Constant("\""), "")])
    }
}

struct IntegerLiteral;
struct FloatLiteral;
struct TimestampLiteral;


struct HexLiteral;
impl <'a> Parser<'a> for HexLiteral {
    type Output = Vec<u8>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<u8>> {
        fn char_to_byte(c: u8) -> Option<u8> {
            match c {
                b'A'...b'F' => Some(c - b'A' + 10),
                b'a'...b'f' => Some(c - b'a' + 10),
                b'0'...b'9' => Some(c - b'0'),
                _ => None,
            }
        }

        let (_, remaining) = try_parse!(Tag("0x").parse(input));
        let bytes = remaining.as_bytes();

        if bytes.is_empty() {
            return ParseResult::Incomplete(vec![(Hint::HexEscape, "")]);
        }

        let mut result = Vec::new();

        for (idx, chunk) in bytes.chunks(2).enumerate() {
            let rest = &input[(idx+1)*2..];
            println!("idx: {}, chunk: {:?}, rest: {:?}", idx, chunk, rest);
            if chunk.len() == 2 {
                let a = chunk[0];
                let b = chunk[1];

                match (char_to_byte(a), char_to_byte(b)) {
                    (Some(a), Some(b)) => result.push(a << 4 | b),
                    (Some(_), None) => return ParseResult::Err(vec![Hint::HexEscape], rest),
                    _ if idx == 0 => return ParseResult::Err(vec![Hint::HexEscape], rest),
                    _ => return ParseResult::Ok(result, rest),
                }
            } else {
                if char_to_byte(chunk[0]).is_some() {
                    return ParseResult::Incomplete(vec![(Hint::HexEscape, rest)]);
                } else if idx == 0 {
                    return ParseResult::Err(vec![Hint::HexEscape], rest);
                } else {
                    return ParseResult::Ok(result, rest);
                }
            }
        }
        ParseResult::Ok(result, "")
    }
}

////////////////////////////////////////////////////////////////////////////////
// SQL Command Parsers
////////////////////////////////////////////////////////////////////////////////

/// Parses a SQL statement into a command.
pub struct Command;
impl <'a> Parser<'a> for Command {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let commands = Help.or_else(ShowTables)
                           .or_else(DescribeTable)
                           .or_else(CreateTable)
                           .or_else(Noop);

        Ignore0(TokenDelimiter)
            .and_then(commands)
            .followed_by(Ignore0(TokenDelimiter))
            .parse(input)
    }
}

/// Parses 1 or more SQL statements into commands.
pub struct Commands1;
impl <'a> Parser<'a> for Commands1 {
    type Output = Vec<command::Command<'a>>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<command::Command<'a>>> {
        Many(Command).parse(input)
    }
}

/// Parses a no-op statement.
struct Noop;
impl <'a> Parser<'a> for Noop {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        match Char(';', "").parse(input) {
            ParseResult::Ok(_, remaining) => ParseResult::Ok(command::Command::Noop, remaining),
            ParseResult::Incomplete(..) => {
                assert!(input.is_empty());
                ParseResult::Incomplete(vec![])
            },
            ParseResult::Err(err, remaining) => ParseResult::Err(err, remaining),
        }
    }
}

/// Parses a HELP statement.
struct Help;
impl <'a> Parser<'a> for Help {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("HELP")
            .and_then(Ignore0(TokenDelimiter))
            .and_then(Char(';', ";"))
            .map(|_| command::Command::Help)
            .parse(input)
    }
}

/// Parses a SHOW TABLES statement.
struct ShowTables;
impl <'a> Parser<'a> for ShowTables {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("SHOW")
            .and_then(Ignore1(TokenDelimiter))
            .and_then(Keyword("TABLES"))
            .and_then(Ignore0(TokenDelimiter))
            .and_then(Char(';', ";"))
            .map(|_| command::Command::ShowTables)
            .parse(input)
    }
}

/// Parses a DESCRIBE TABLE statement.
struct DescribeTable;
impl <'a> Parser<'a> for DescribeTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("DESCRIBE")
            .and_then(Ignore1(TokenDelimiter))
            .and_then(Keyword("TABLE"))
            .and_then(Ignore1(TokenDelimiter))
            .and_then(TableName).map(|table| command::Command::DescribeTable { table: table })
            .followed_by(Ignore0(TokenDelimiter))
            .followed_by(Char(';', ";"))
            .parse(input)
    }
}

struct DataType;
impl <'a> Parser<'a> for DataType {
    type Output = kudu::DataType;
    fn parse(&self, input: &'a str) -> ParseResult<'a, kudu::DataType> {
        (Keyword("BOOL").map(|_| kudu::DataType::Bool))
                   .or_else(Keyword("INT8").map(|_| kudu::DataType::Int8))
                   .or_else(Keyword("INT16").map(|_| kudu::DataType::Int16))
                   .or_else(Keyword("INT32").map(|_| kudu::DataType::Int32))
                   .or_else(Keyword("INT64").map(|_| kudu::DataType::Int64))
                   .or_else(Keyword("TIMESTAMP").map(|_| kudu::DataType::Timestamp))
                   .or_else(Keyword("FLOAT").map(|_| kudu::DataType::Float))
                   .or_else(Keyword("DOUBLE").map(|_| kudu::DataType::Double))
                   .or_else(Keyword("BINARY").map(|_| kudu::DataType::Binary))
                   .or_else(Keyword("STRING").map(|_| kudu::DataType::String))
                   .parse(input)
    }
}

struct EncodingType;
impl <'a> Parser<'a> for EncodingType {
    type Output = kudu::EncodingType;
    fn parse(&self, input: &'a str) -> ParseResult<'a, kudu::EncodingType> {
        Keyword("ENCODING").and_then(Ignore1(TokenDelimiter))
                       .and_then(Keyword("DEFAULT").map(|_| kudu::EncodingType::Default)
                        .or_else(Keyword("PLAIN").map(|_| kudu::EncodingType::Plain))
                        .or_else(Keyword("PREFIX").map(|_| kudu::EncodingType::Prefix))
                        .or_else(Keyword("GROUPVARINT").map(|_| kudu::EncodingType::GroupVarint))
                        .or_else(Keyword("RUNLENGTH").map(|_| kudu::EncodingType::RunLength))
                        .or_else(Keyword("DICTIONARY").map(|_| kudu::EncodingType::Dictionary))
                        .or_else(Keyword("BITSHUFFLE").map(|_| kudu::EncodingType::BitShuffle)))
                       .parse(input)
    }
}

struct CompressionType;
impl <'a> Parser<'a> for CompressionType {
    type Output = kudu::CompressionType;
    fn parse(&self, input: &'a str) -> ParseResult<'a, kudu::CompressionType> {
        Keyword("COMPRESSION").and_then(Ignore1(TokenDelimiter))
                              .and_then(Keyword("DEFAULT").map(|_| kudu::CompressionType::Default)
                               .or_else(Keyword("NONE").map(|_| kudu::CompressionType::None))
                               .or_else(Keyword("SNAPPY").map(|_| kudu::CompressionType::Snappy))
                               .or_else(Keyword("LZ4").map(|_| kudu::CompressionType::Lz4))
                               .or_else(Keyword("ZLIB").map(|_| kudu::CompressionType::Zlib)))
                              .parse(input)
    }
}

struct BlockSize;
impl <'a> Parser<'a> for BlockSize {
    type Output = i32;
    fn parse(&self, input: &'a str) -> ParseResult<'a, i32> {
        Keyword("BLOCK").and_then(Ignore1(TokenDelimiter))
                        .and_then(Keyword("SIZE"))
                        .and_then(Ignore1(TokenDelimiter))
                        .and_then(I32)
                        .parse(input)
    }
}

struct CreateColumn;
impl <'a> Parser<'a> for CreateColumn {
    type Output = command::CreateColumn<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::CreateColumn<'a>> {
        let (name, remaining) = try_parse!(ColumnName.parse(input));
        let (data_type, remaining) =
            try_parse!(Ignore1(TokenDelimiter).and_then(DataType).parse(remaining));

        let (encoding_type, remaining) =
            try_parse!(Optional(Ignore1(TokenDelimiter).and_then(EncodingType)).parse(remaining));
        let (compression_type, remaining) =
            try_parse!(Optional(Ignore1(TokenDelimiter).and_then(CompressionType)).parse(remaining));
        let (block_size, remaining) =
            try_parse!(Optional(Ignore1(TokenDelimiter).and_then(BlockSize)).parse(remaining));

        ParseResult::Ok(command::CreateColumn::new(name,
                                                   data_type,
                                                   encoding_type,
                                                   compression_type,
                                                   block_size),
                        remaining)
    }
}

struct PrimaryKey;
impl <'a> Parser<'a> for PrimaryKey {
    type Output = Vec<&'a str>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<&'a str>> {
        Keyword("PRIMARY").and_then(Ignore1(TokenDelimiter))
                          .and_then(Keyword("KEY"))
                          .and_then(Ignore0(TokenDelimiter))
                          .and_then(Char('(', "("))
                          .and_then(Ignore0(TokenDelimiter))
                          .and_then(Delimited1(ColumnName, Ignore0(TokenDelimiter).and_then(Char(',', ","))
                                                                                  .and_then(Ignore0(TokenDelimiter))))
                          .followed_by(Ignore0(TokenDelimiter))
                          .followed_by(Char(')', ")"))
                          .parse(input)
    }
}


struct CreateTable;
impl <'a> Parser<'a> for CreateTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let (name, remaining) = try_parse!(
            Keyword("CREATE").and_then(Ignore1(TokenDelimiter))
                             .and_then(Keyword("TABLE"))
                             .and_then(Ignore1(TokenDelimiter))
                             .and_then(TableName)
                             .followed_by(Ignore1(TokenDelimiter))
                             .parse(input));
        let (columns, remaining) = try_parse!(
            Char('(', "(").and_then(Delimited1(CreateColumn,
                                               Ignore0(TokenDelimiter).followed_by(Char(',', ","))
                                                                      .followed_by(Ignore1(TokenDelimiter))))
                    .followed_by(Ignore0(TokenDelimiter))
                    .followed_by(Char(')', ")"))
                    .followed_by(Ignore0(TokenDelimiter))
                    .parse(remaining));
        let (primary_key, remaining) = try_parse!(PrimaryKey.followed_by(Ignore0(TokenDelimiter))
                                                            .followed_by(Char(';', ";"))
                                                            .parse(remaining));

        ParseResult::Ok(command::Command::CreateTable {
            name: name,
            columns: columns,
            primary_key: primary_key,
            range_partition: None,
            hash_partitions: Vec::new(),
        }, remaining)
    }
}

#[cfg(test)]
mod test {

    use std::borrow::Cow;

    use kudu;

    use super::{
        BlockComment,
        Char,
        ColumnName,
        Command,
        CreateColumn,
        CreateTable,
        DataType,
        Delimited1,
        DescribeTable,
        DoubleQuotedStringLiteral,
        HexLiteral,
        Hint,
        I32,
        Ignore0,
        Keyword,
        LineComment,
        Noop,
        Parser,
        ParseResult,
        ShowTables,
        TokenDelimiter,
    };
    use command;

    fn incomplete<'a, T>(hint: Hint<'a>, rest: &'a str) -> ParseResult<'a, T> {
        ParseResult::Incomplete(vec![(hint, rest)])
    }

    fn error<'a, T>(hint: Hint<'a>, rest: &'a str) -> ParseResult<'a, T> {
        ParseResult::Err(vec![hint], rest)
    }

    #[test]
    fn test_try_parse() {
        fn try_parse<T>(result: ParseResult<T>) -> ParseResult<T> {
            let (value, remaining) = try_parse!(result);
            ParseResult::Ok(value, remaining)
        }

        for result in vec![ParseResult::Ok(13, "fuzz"),
                           incomplete(Hint::Constant("foo"), "fuzz"),
                           error(Hint::Constant("SELECT"), "fuzz")] {
            assert_eq!(result.clone(), try_parse(result));
        }
    }

    #[test]
    fn test_token_delimiter() {
        let parser = TokenDelimiter;
        assert_eq!(parser.parse("  "), ParseResult::Ok("  ", ""));
        assert_eq!(parser.parse(" a "), ParseResult::Ok(" ", "a "));

        assert_eq!(parser.parse(""), incomplete(Hint::Constant(" "), ""));

        assert_eq!(parser.parse("S"), error(Hint::Constant(" "), "S"));
    }

    #[test]
    fn test_keyword() {
        let parser = Keyword("FOO");
        assert_eq!(parser.parse("FOO"), ParseResult::Ok("FOO", ""));
        assert_eq!(parser.parse("foo"), ParseResult::Ok("foo", ""));
        assert_eq!(parser.parse("FoO"), ParseResult::Ok("FoO", ""));
        assert_eq!(parser.parse("foo bar baz"), ParseResult::Ok("foo", " bar baz"));

        assert_eq!(parser.parse("fo"), incomplete(Hint::Constant("O"), ""));

        assert_eq!(parser.parse("fub"), error(Hint::Constant("OO"), "ub"));
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
                   incomplete(Hint::Constant("*/"), " fuzz"));

        assert_eq!(parser.parse(""), incomplete(Hint::Constant("/*"), ""));
        assert_eq!(parser.parse("/"), incomplete(Hint::Constant("/*"), "/"));

        assert_eq!(parser.parse("*/"), error(Hint::Constant("/*"), "*/"));
        assert_eq!(parser.parse(" foo"), error(Hint::Constant("/*"), " foo"));
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

        assert_eq!(parser.parse("-"), incomplete(Hint::Constant("--"), "-"));

        assert_eq!(parser.parse(" --"), error(Hint::Constant("--"), " --"));
        assert_eq!(parser.parse("- "), error(Hint::Constant("--"), "- "));
    }

    #[test]
    fn test_noop() {
        let parser = Noop;
        assert_eq!(parser.parse(";"),
                   ParseResult::Ok(command::Command::Noop, ""));
        assert_eq!(parser.parse("SHOW TABLES;"),
                   error(Hint::Constant(""), "SHOW TABLES;"));
    }

    #[test]
    fn test_show_tables() {
        let parser = ShowTables;
        assert_eq!(parser.parse("SHOW TABLES ;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
        assert_eq!(parser.parse("shOw TABLES;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
        assert_eq!(parser.parse("shOw \t\r\nTABLES;next command"),
                   ParseResult::Ok(command::Command::ShowTables, "next command"));

        assert_eq!(parser.parse("SHOW--mycoment ; foo bar\nTABLES;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
    }

    #[test]
    fn test_describe_table() {
        let parser = DescribeTable;
        assert_eq!(parser.parse("DESCRIBE TABLE _foo1_bAr2;"),
                   ParseResult::Ok(command::Command::DescribeTable { table: "_foo1_bAr2" }, ""));

        assert_eq!(parser.parse("DESCRIBE TABLE \n\t\r -- some comment\n_______--another comment\n ; SELECT"),
                   ParseResult::Ok(command::Command::DescribeTable { table: "_______" }, " SELECT"));

        assert_eq!(parser.parse("DESCRIBE TABLE -- t;\n"),
                   incomplete(Hint::Table(""), ""));
        assert_eq!(parser.parse("DESCRIBE/* */TABLE/* foo */foo"),
                   incomplete(Hint::Table("foo"), "foo"));

        assert_eq!(parser.parse("DESCRIBETABLE foo;"),
                   error(Hint::Constant(" "), "TABLE foo;"));
        assert_eq!(parser.parse("DESCRIBE/**/TABLE foo."),
                   error(Hint::Constant(";"), "."));
    }

    #[test]
    fn test_command() {
        let parser = Command;
        assert_eq!(parser.parse("  DESCRIBE TABLE _foo_bAr;"),
                   ParseResult::Ok(command::Command::DescribeTable { table: "_foo_bAr" }, ""));
        assert_eq!(parser.parse(" \n Show Tables;  SELECT"),
                   ParseResult::Ok(command::Command::ShowTables, "SELECT"));

        assert_eq!(parser.parse("  show t"),
                   incomplete(Hint::Constant("ABLES"), ""));
        assert_eq!(parser.parse("   describe t"),
                   incomplete(Hint::Constant("ABLE"), ""));

        assert_eq!(parser.parse("describe --\n foo"),
                   error(Hint::Constant("TABLE"), "foo"));
    }

    #[test]
    fn test_i32() {
        let parser = I32;
        assert_eq!(parser.parse("1234"), ParseResult::Ok(1234, ""));
        assert_eq!(parser.parse("1234 "), ParseResult::Ok(1234, " "));
        assert_eq!(parser.parse("123abc"), ParseResult::Ok(123, "abc"));

        assert_eq!(parser.parse(""), incomplete(Hint::Integer, ""));

        assert_eq!(parser.parse("abc"), error(Hint::Integer, "abc"));
    }

    #[test]
    fn test_column_name() {
        let parser = ColumnName;
        assert_eq!(parser.parse("foo "), ParseResult::Ok("foo", " "));
        assert_eq!(parser.parse("12foo/*"), ParseResult::Ok("12foo", "/*"));
        assert_eq!(parser.parse("_fooBar "), ParseResult::Ok("_fooBar", " "));

        assert_eq!(parser.parse(""), incomplete(Hint::Column(""), ""));
        assert_eq!(parser.parse("fuzz"), incomplete(Hint::Column("fuzz"), "fuzz"));

        assert_eq!(parser.parse("()"), error(Hint::Column(""), "()"));
    }

    #[test]
    fn test_data_type() {
        let parser = DataType;
        assert_eq!(parser.parse("BOOL"), ParseResult::Ok(kudu::DataType::Bool, ""));
        assert_eq!(parser.parse("bool"), ParseResult::Ok(kudu::DataType::Bool, ""));
        assert_eq!(parser.parse("int32"), ParseResult::Ok(kudu::DataType::Int32, ""));
        assert_eq!(parser.parse("double bubble"),
                   ParseResult::Ok(kudu::DataType::Double, " bubble"));

        assert_eq!(parser.parse("f"), incomplete(Hint::Constant("LOAT"), ""));
        assert_eq!(parser.parse("fuzz"), error(Hint::Constant("LOAT"), "uzz"));
    }

    #[test]
    fn test_create_column() {
        let parser = CreateColumn;
        assert_eq!(parser.parse("foo int32"),
                   ParseResult::Ok(command::CreateColumn::new("foo",
                                                              kudu::DataType::Int32,
                                                              None, None, None), ""));
        assert_eq!(parser.parse("foo int32 BLOCK SIZE 4096;"),
                   ParseResult::Ok(command::CreateColumn::new("foo",
                                                              kudu::DataType::Int32,
                                                              None, None, Some(4096)), ";"));

        assert_eq!(parser.parse("foo timestamp ENCODING runlength COMPRESSION zlib BLOCK SIZE 99;"),
                   ParseResult::Ok(command::CreateColumn::new("foo",
                                                              kudu::DataType::Timestamp,
                                                              Some(kudu::EncodingType::RunLength),
                                                              Some(kudu::CompressionType::Zlib),
                                                              Some(99)), ";"));
    }

    #[test]
    fn test_delimited() {
        let parser = Delimited1(Keyword("FUZZ"),
                                Ignore0(TokenDelimiter).followed_by(Char(',', ","))
                                                       .followed_by(Ignore0(TokenDelimiter)));
        assert_eq!(parser.parse("fuzz, fuzz    -- sdf \n , fuzz,fuzz,fuzz;"),
                   ParseResult::Ok(vec!["fuzz", "fuzz", "fuzz", "fuzz", "fuzz"], ";"));
        assert_eq!(parser.parse("fuzz,; fuzz"),
                   ParseResult::Ok(vec!["fuzz"], ",; fuzz"));

        assert_eq!(parser.parse(",; fuzz"),
                   error(Hint::Constant("FUZZ"), ",; fuzz"));
    }

    #[test]
    fn test_create_table() {
        let parser = CreateTable;
        assert_eq!(parser.parse("create table t (a int32, b timestamp, c string) primary key (a, c);"),
                   ParseResult::Ok(command::Command::CreateTable {
                       name: "t",
                       columns: vec![
                           command::CreateColumn::new("a", kudu::DataType::Int32, None, None, None),
                           command::CreateColumn::new("b", kudu::DataType::Timestamp, None, None, None),
                           command::CreateColumn::new("c", kudu::DataType::String, None, None, None),
                       ],
                       primary_key: vec!["a", "c"],
                       range_partition: None,
                       hash_partitions: Vec::new(),
                   }, ""));
    }

    #[test]
    fn test_double_quoted_string_literal() {
        let parser = DoubleQuotedStringLiteral;
        assert_eq!(parser.parse(r#""fuzz""#),
                   ParseResult::Ok(Cow::Borrowed("fuzz"), ""));
        assert_eq!(parser.parse(r#""fuzz"wuzz"#),
                   ParseResult::Ok(Cow::Borrowed("fuzz"), "wuzz"));
        assert_eq!(parser.parse(r#""fu"zz"wuzz"#),
                   ParseResult::Ok(Cow::Borrowed("fu"), r#"zz"wuzz"#));
        assert_eq!(parser.parse(r#""fu\\\n\t\"\""zz"wuzz"#),
                   ParseResult::Ok(Cow::Owned("fu\\\n\t\"\"".to_owned()), r#"zz"wuzz"#));

        assert_eq!(parser.parse("foo"),
                   ParseResult::Err(vec![Hint::Constant("\"")], "foo"));
        assert_eq!(parser.parse(r#""foo\b""#),
                   ParseResult::Err(vec![Hint::CharEscape], "b\""));


        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Constant("\""), "")]));
        assert_eq!(parser.parse(r#""foo"#),
                   ParseResult::Incomplete(vec![(Hint::Constant("\""), "")]));
    }

    #[test]
    fn test_hex_literal() {
        let parser = HexLiteral;
        assert_eq!(parser.parse("0x42"),
                   ParseResult::Ok(vec![66], ""));
        assert_eq!(parser.parse("0x00010203"),
                   ParseResult::Ok(vec![0, 1, 2, 3], ""));
        assert_eq!(parser.parse("0x0123456789abcdefABCDEF"),
                   ParseResult::Ok(vec![1, 35, 69, 103, 137, 171, 205, 239, 171, 205, 239], ""));
        assert_eq!(parser.parse("0x0123zab"),
                   ParseResult::Ok(vec![1, 35], "zab"));

        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Constant("0x"), "")]));
        assert_eq!(parser.parse("0"),
                   ParseResult::Incomplete(vec![(Hint::Constant("0x"), "0")]));
        assert_eq!(parser.parse("0x"),
                   ParseResult::Incomplete(vec![(Hint::HexEscape, "")]));
        assert_eq!(parser.parse("0x0"),
                   ParseResult::Incomplete(vec![(Hint::HexEscape, "0")]));
        assert_eq!(parser.parse("0x012"),
                   ParseResult::Incomplete(vec![(Hint::HexEscape, "2")]));
        assert_eq!(parser.parse("0x01234"),
                   ParseResult::Incomplete(vec![(Hint::HexEscape, "4")]));

        assert_eq!(parser.parse("f"),
                   ParseResult::Err(vec![Hint::Constant("0x")], "f"));
        assert_eq!(parser.parse("00"),
                   ParseResult::Err(vec![Hint::Constant("0x")], "00"));
        assert_eq!(parser.parse("0xzab"),
                   ParseResult::Err(vec![Hint::HexEscape], "zab"));
        assert_eq!(parser.parse("0x0zab"),
                   ParseResult::Err(vec![Hint::HexEscape], "0zab"));
        assert_eq!(parser.parse("0x01234zab"),
                   ParseResult::Err(vec![Hint::HexEscape], "4zab"));
    }
}
