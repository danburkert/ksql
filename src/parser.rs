//! SQL Parsers
//!
//! This module defines parsers for kudusql's SQL dialect. The parsers are
//! written in the parser combinator style, and are heavily influenced by Nom
//! (but with fewer macros). Nom wasn't appropriate because it does not have the
//! ability to return a hint as the result of an incomplete parse.

use std::borrow::Cow;

use chrono;
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

    /// Hint with a positive integer value.
    PosInteger,

    /// Hint with a floating point value.
    Float,

    /// Hint witha timestamp value.
    Timestamp,

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

/// Applies the parser until failure, returning the vector of results.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
struct Many0<P>(P);
impl <'a, T, P> Parser<'a> for Many0<P> where P: Parser<'a, Output=T> {
    type Output = Vec<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<T>> {
        let mut output = Vec::new();
        let mut remaining = input;
        while let ParseResult::Ok(t, r) = self.0.parse(remaining) {
            output.push(t);
            remaining = r;
        }
        ParseResult::Ok(output, remaining)
    }
}

/// Applies the parser at least once, returning the vector of results.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
struct Many1<P>(P);
impl <'a, T, P> Parser<'a> for Many1<P> where P: Parser<'a, Output=T> {
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
/// TODO: is this right?  I think the hints may be a little messed up?
struct Delimited0<P, D>(P, D);
impl <'a, T, P, D> Parser<'a> for Delimited0<P, D>
where P: Parser<'a, Output=T>,
      D: Parser<'a> {
    type Output = Vec<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<T>> {
        let mut output = Vec::new();
        let mut remaining;
        if let ParseResult::Ok(t, r) = self.0.parse(input) {
            remaining = r;
            output.push(t);
        } else {
            return ParseResult::Ok(output, input);
        }

        loop {
            if let ParseResult::Ok(_, r) = self.1.parse(remaining) {
                let (t, r) = try_parse!(self.0.parse(r));
                remaining = r;
                output.push(t);
            } else {
                return ParseResult::Ok(output, remaining)
            }
        }
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
                let (t, r) = try_parse!(self.0.parse(r));
                remaining = r;
                output.push(t);
            } else {
                return ParseResult::Ok(output, remaining)
            }
        }
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
            ParseResult::Err(mut error1, remaining1) => {
                match self.1.parse(input) {
                    ParseResult::Ok(t, remaining) => ParseResult::Ok(t, remaining),
                    ParseResult::Incomplete(hints2) => ParseResult::Incomplete(hints2),
                    ParseResult::Err(error2, remaining2) => {
                        // Both parses errored. Return the error result for the
                        // parse which made the most progress.
                        if remaining1.len() == remaining2.len() {
                            debug_assert!(remaining1 == remaining2);
                            error1.extend_from_slice(&error2);
                            ParseResult::Err(error1, remaining1)
                        } else if remaining1.len() < remaining2.len() {
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

/// Parses an optional clause with mandatory leading whitespace.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct OptionalClause<P>(P);
impl <'a, P, T> Parser<'a> for OptionalClause<P>
where P: Parser<'a, Output=T> {
    type Output = Option<T>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Option<T>> {
        if let ParseResult::Ok(_, remaining1) = Chomp1.parse(input) {
            match self.0.parse(remaining1) {
                ParseResult::Ok(v, remaining2) => ParseResult::Ok(Some(v), remaining2),
                ParseResult::Incomplete(hints) => {
                    if hints.iter().any(|hint| hint.1.len() < remaining1.len()) {
                        ParseResult::Incomplete(hints)
                    } else {
                        ParseResult::Ok(None, input)
                    }
                },
                ParseResult::Err(hints, remaining2) => {
                    if remaining2.len() < remaining1.len() {
                        ParseResult::Err(hints, remaining2)
                    } else {
                        ParseResult::Ok(None, input)
                    }
                },
            }
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
            Err(_) => ParseResult::Err(vec![Hint::Integer], input),
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
        // Limit to 1 hint so that comments aren't given as a hint, unless they
        // are a partial match.
        match TakeWhile1(is_multispace, Hint::Constant(" ")).or_else(LineComment)
                                                            .or_else(BlockComment)
                                                            .parse(input) {
            ParseResult::Ok(value, remaining) => ParseResult::Ok(value, remaining),
            ParseResult::Incomplete(mut hints) => {
                hints.truncate(1);
                ParseResult::Incomplete(hints)
            },
            ParseResult::Err(mut hints, remaining) => {
                hints.truncate(1);
                ParseResult::Err(hints, remaining)
            }
        }
    }
}

/// Parses 0 or more token delimiters.
struct Chomp0;
impl <'a> Parser<'a> for Chomp0 {
    type Output = ();
    fn parse(&self, input: &'a str) -> ParseResult<'a, ()> {
        Ignore0(TokenDelimiter).parse(input)
    }
}

/// Parses 1 or more token delimiters.
struct Chomp1;
impl <'a> Parser<'a> for Chomp1 {
    type Output = ();
    fn parse(&self, input: &'a str) -> ParseResult<'a, ()> {
        Ignore1(TokenDelimiter).parse(input)
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

struct BoolLiteral;
impl <'a> Parser<'a> for BoolLiteral {
    type Output = bool;
    fn parse(&self, input: &'a str) -> ParseResult<'a, bool> {
        Tag("true").map(|_| true)
                   .or_else(Tag("false").map(|_| false))
                   .parse(input)
    }
}

struct IntLiteral;
impl <'a> Parser<'a> for IntLiteral {
    type Output = i64;
    fn parse(&self, input: &'a str) -> ParseResult<'a, i64> {
        let remaining = match Optional(Char('-', "")).and_then(TakeWhile1(is_numeric, Hint::Integer))
                                                     .parse(input) {
            ParseResult::Ok(_, remaining) => remaining,
            ParseResult::Incomplete(..) => return ParseResult::Incomplete(vec![(Hint::Integer, input)]),
            ParseResult::Err(..) => return ParseResult::Err(vec![Hint::Integer], input),
        };

        let (int, rest) = input.split_at(input.len() - remaining.len());
        match int.parse::<i64>() {
            Ok(int) => ParseResult::Ok(int, rest),
            Err(_) => ParseResult::Err(vec![Hint::Integer], input),
        }
    }
}

struct PosIntLiteral;
impl <'a> Parser<'a> for PosIntLiteral {
    type Output = u64;
    fn parse(&self, input: &'a str) -> ParseResult<'a, u64> {
        let (int, rest) = try_parse!(TakeWhile1(is_numeric, Hint::PosInteger).parse(input)); 
        match int.parse::<u64>() {
            Ok(int) => ParseResult::Ok(int, rest),
            Err(_) => ParseResult::Err(vec![Hint::PosInteger], input),
        }
    }
}

struct FloatLiteral;
impl <'a> Parser<'a> for FloatLiteral {
    type Output = f64;
    fn parse(&self, input: &'a str) -> ParseResult<'a, f64> {
        if input.is_empty() { return ParseResult::Incomplete(vec![(Hint::Float, input)]); }
        let remaining = match Optional(Char('-', ""))
                                .and_then(TakeWhile0(is_numeric))
                                .and_then(Char('.', "").and_then(TakeWhile0(is_numeric)))
                                .and_then(Optional(Char('e', "").and_then(Optional(IntLiteral))))
                                .parse(input) {
            ParseResult::Ok(_, remaining) => remaining,
            ParseResult::Incomplete(..) => return ParseResult::Incomplete(vec![(Hint::Float, input)]),
            ParseResult::Err(..) => return ParseResult::Err(vec![Hint::Float], input),
        };

        let (int, rest) = input.split_at(input.len() - remaining.len());

        match int.parse::<f64>() {
            Ok(int) => ParseResult::Ok(int, rest),
            Err(_) => ParseResult::Err(vec![Hint::Float], input),
        }
    }
}

struct TimestampLiteral;
impl <'a> Parser<'a> for TimestampLiteral {
    type Output = chrono::DateTime<chrono::FixedOffset>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, chrono::DateTime<chrono::FixedOffset>> {

        // 1990-12-31T23:59:60.0123Z
        // or
        // 1996-12-19T16:39:57-08:00

        if input.is_empty() { return ParseResult::Incomplete(vec![(Hint::Timestamp, input)]); }
        let remaining = match TakeWhile1(is_numeric, Hint::Constant(""))
                                 .and_then(Char('-', ""))
                                 .and_then(TakeWhile1(is_numeric, Hint::Constant("")))
                                 .and_then(Char('-', ""))
                                 .and_then(TakeWhile1(is_numeric, Hint::Constant("")))
                                 .and_then(Char('T', ""))
                                 .and_then(TakeWhile1(is_numeric, Hint::Constant("")))
                                 .and_then(Char(':', ""))
                                 .and_then(TakeWhile1(is_numeric, Hint::Constant("")))
                                 .and_then(Char(':', ""))
                                 .and_then(TakeWhile1(is_numeric, Hint::Constant("")))
                                 .and_then(Optional(Char('.', "").and_then(TakeWhile1(is_numeric, Hint::Constant("")))))
                                 .and_then(Char('Z', "").or_else(Char('-', "").followed_by(TakeWhile1(is_numeric, Hint::Constant("")))
                                                                              .followed_by(Char(':', ""))
                                                                              .followed_by(TakeWhile1(is_numeric, Hint::Constant("")))))
                                 .parse(input) {
            ParseResult::Ok(_, remaining) => remaining,
            ParseResult::Incomplete(..) => return ParseResult::Incomplete(vec![(Hint::Timestamp, input)]),
            ParseResult::Err(..) => return ParseResult::Err(vec![Hint::Timestamp], input),
        };

        let timestamp = &input[..input.len() - remaining.len()];

        match chrono::DateTime::parse_from_rfc3339(timestamp) {
            Ok(timestamp) => ParseResult::Ok(timestamp, remaining),
            Err(_) => ParseResult::Err(vec![Hint::Timestamp], input),
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

struct Literal;
impl <'a> Parser<'a> for Literal {
    type Output = command::Literal<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Literal<'a>> {
        BoolLiteral.map(|b| command::Literal::Bool(b))
                   .or_else(HexLiteral.map(|b| command::Literal::Binary(b)))
                   .or_else(TimestampLiteral.map(|t| command::Literal::Timestamp(t)))
                   .or_else(FloatLiteral.map(|f| command::Literal::Float(f)))
                   .or_else(IntLiteral.map(|i| command::Literal::Integer(i)))
                   .or_else(DoubleQuotedStringLiteral.map(|s| command::Literal::String(s)))
                   .or_else(Keyword("NULL").map(|_| command::Literal::Null))
                   .parse(input)
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
                           .or_else(ShowMasters)
                           .or_else(ShowTabletServers)
                           .or_else(ShowTableTablets)
                           .or_else(ShowTableReplicas)
                           .or_else(DescribeTable)
                           .or_else(Select)
                           .or_else(Insert)
                           .or_else(CreateTable)
                           .or_else(DropTable)
                           .or_else(AlterTable)
                           .or_else(Noop);

        Chomp0.and_then(commands)
              .followed_by(Chomp0)
              .parse(input)
    }
}

pub struct Commands1;
impl <'a> Parser<'a> for Commands1 {
    type Output = Vec<command::Command<'a>>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<command::Command<'a>>> {
        Many1(Command).parse(input)
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
            .and_then(Chomp0)
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
            .and_then(Chomp1)
            .and_then(Keyword("TABLES"))
            .and_then(Chomp0)
            .and_then(Char(';', ";"))
            .map(|_| command::Command::ShowTables)
            .parse(input)
    }
}

/// Parses a SHOW MASTERS statement.
struct ShowMasters;
impl <'a> Parser<'a> for ShowMasters {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("SHOW")
            .and_then(Chomp1)
            .and_then(Keyword("MASTERS"))
            .and_then(Chomp0)
            .and_then(Char(';', ";"))
            .map(|_| command::Command::ShowMasters)
            .parse(input)
    }
}

/// Parses a SHOW TABLET SERVERS statement.
struct ShowTabletServers;
impl <'a> Parser<'a> for ShowTabletServers {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("SHOW")
            .and_then(Chomp1)
            .and_then(Keyword("TABLET"))
            .and_then(Chomp1)
            .and_then(Keyword("SERVERS"))
            .and_then(Chomp0)
            .and_then(Char(';', ";"))
            .map(|_| command::Command::ShowTabletServers)
            .parse(input)
    }
}

/// Parses a SHOW TABLETS FOR TABLE statement.
struct ShowTableTablets;
impl <'a> Parser<'a> for ShowTableTablets {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("SHOW")
            .and_then(Chomp1)
            .and_then(Keyword("TABLETS"))
            .and_then(Chomp1)
            .and_then(Keyword("OF"))
            .and_then(Chomp1)
            .and_then(Keyword("TABLE"))
            .and_then(Chomp1)
            .and_then(TableName).map(|table| command::Command::ShowTableTablets { table: table })
            .followed_by(Chomp0)
            .followed_by(Char(';', ";"))
            .parse(input)
    }
}

/// Parses a SHOW TABLET REPLICAS FOR TABLE statement.
struct ShowTableReplicas;
impl <'a> Parser<'a> for ShowTableReplicas {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("SHOW")
            .and_then(Chomp1)
            .and_then(Keyword("TABLET"))
            .and_then(Chomp1)
            .and_then(Keyword("REPLICAS"))
            .and_then(Chomp1)
            .and_then(Keyword("OF"))
            .and_then(Chomp1)
            .and_then(Keyword("TABLE"))
            .and_then(Chomp1)
            .and_then(TableName).map(|table| command::Command::ShowTableReplicas { table: table })
            .followed_by(Chomp0)
            .followed_by(Char(';', ";"))
            .parse(input)
    }
}

/// Parses a DESCRIBE TABLE statement.
struct DescribeTable;
impl <'a> Parser<'a> for DescribeTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("DESCRIBE")
            .and_then(Chomp1)
            .and_then(Keyword("TABLE"))
            .and_then(Chomp1)
            .and_then(TableName).map(|table| command::Command::DescribeTable { table: table })
            .followed_by(Chomp0)
            .followed_by(Char(';', ";"))
            .parse(input)
    }
}

struct DropTable;
impl <'a> Parser<'a> for DropTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        Keyword("DROP")
            .and_then(Chomp1)
            .and_then(Keyword("TABLE"))
            .and_then(Chomp1)
            .and_then(TableName).map(|table| command::Command::DropTable { table: table })
            .followed_by(Chomp0)
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
        Keyword("ENCODING").and_then(Chomp1)
                       .and_then(Keyword("AUTO").map(|_| kudu::EncodingType::Auto)
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
        Keyword("COMPRESSION").and_then(Chomp1)
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
        Keyword("BLOCK").and_then(Chomp1)
                        .and_then(Keyword("SIZE"))
                        .and_then(Chomp1)
                        .and_then(I32)
                        .parse(input)
    }
}

struct Nullable;
impl <'a> Parser<'a> for Nullable {
    type Output = bool;
    fn parse(&self, input: &'a str) -> ParseResult<'a, bool> {
        Keyword("NULLABLE").map(|_| true)
                           .or_else(Keyword("NOT").and_then(Chomp1)
                                                  .and_then(Keyword("NULL"))
                                                  .map(|_| false))
                           .parse(input)
    }
}

struct ColumnSpec;
impl <'a> Parser<'a> for ColumnSpec {
    type Output = command::ColumnSpec<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::ColumnSpec<'a>> {
        let (name, remaining) = try_parse!(ColumnName.parse(input));
        let (data_type, remaining) = try_parse!(Chomp1.and_then(DataType).parse(remaining));

        let (nullable, remaining) = try_parse!(OptionalClause(Nullable).parse(remaining));
        let (encoding_type, remaining) = try_parse!(OptionalClause(EncodingType).parse(remaining));
        let (compression_type, remaining) = try_parse!(OptionalClause(CompressionType).parse(remaining));
        let (block_size, remaining) = try_parse!(OptionalClause(BlockSize).parse(remaining));

        ParseResult::Ok(command::ColumnSpec::new(name,
                                                 data_type,
                                                 nullable,
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
        Keyword("PRIMARY").and_then(Chomp1)
                          .and_then(Keyword("KEY"))
                          .and_then(Chomp0)
                          .and_then(Char('(', "("))
                          .and_then(Chomp0)
                          .and_then(Delimited1(ColumnName, Chomp0.and_then(Char(',', ","))
                                                                 .and_then(Chomp0)))
                          .followed_by(Chomp0)
                          .followed_by(Char(')', ")"))
                          .parse(input)
    }
}

// ((0, "foo"), (456, "bar"))
struct RangeBound;
impl <'a> Parser<'a> for RangeBound {
    type Output = (Vec<command::Literal<'a>>, Vec<command::Literal<'a>>);
    fn parse(&self, input: &'a str) -> ParseResult<'a, (Vec<command::Literal<'a>>,
                                                        Vec<command::Literal<'a>>)> {
        let (lower_bound, remaining) = try_parse!(Char('(', "(").and_then(Chomp0)
                                                                .and_then(Row)
                                                                .followed_by(Chomp0)
                                                                .followed_by(Char(',', ","))
                                                                .followed_by(Chomp0)
                                                                .parse(input));
        let (upper_bound, remaining) = try_parse!(Row.followed_by(Chomp0)
                                                     .followed_by(Char(')', ")"))
                                                     .parse(remaining));
        ParseResult::Ok((lower_bound, upper_bound), remaining)
    }
}

// RANGE (a, b, c) SPLIT ROWS (123), (456), RANGE BOUNDS ((0), (250)), ((400), (800));
struct RangePartition;
impl <'a> Parser<'a> for RangePartition {
    type Output = command::RangePartition<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::RangePartition<'a>> {
        let (columns, remaining) =
            try_parse!(Keyword("RANGE").and_then(Chomp0)
                                       .and_then(Char('(', "("))
                                       .and_then(Chomp0)
                                       .and_then(Delimited1(ColumnName, Chomp0.and_then(Char(',', ","))
                                                                              .and_then(Chomp0)))
                                       .followed_by(Chomp0)
                                       .followed_by(Char(')', ")"))
                                       .parse(input));

        let (split_rows, remaining) =
            try_parse!(OptionalClause(Keyword("SPLIT").and_then(Chomp1)
                                                      .and_then(Keyword("ROWS"))
                                                      .and_then(Chomp0)
                                                      .and_then(Delimited1(Row, Chomp0.and_then(Char(',', ","))
                                                                                      .and_then(Chomp0))))
                       .map(|o| o.unwrap_or(Vec::new()))
                       .parse(remaining));

        let (bounds, remaining) =
            try_parse!(OptionalClause(Keyword("BOUNDS").and_then(Chomp1)
                                                       .and_then(Delimited1(RangeBound,
                                                                            Chomp0.and_then(Char(',', ","))
                                                                                  .and_then(Chomp0))))
                       .map(|o| o.unwrap_or(Vec::new()))
                       .parse(remaining));


        ParseResult::Ok(command::RangePartition::new(columns, split_rows, bounds), remaining)
    }
}

// HASH (a, b, c) WITH SEED 99 INTO 4 BUCKETS
struct HashPartition;
impl <'a> Parser<'a> for HashPartition {
    type Output = command::HashPartition<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::HashPartition<'a>> {
        let (columns, remaining) =
            try_parse!(Keyword("HASH").and_then(Chomp0)
                                      .and_then(Char('(', "("))
                                      .and_then(Chomp0)
                                      .and_then(Delimited1(ColumnName, Chomp0.and_then(Char(',', ","))
                                                                             .and_then(Chomp0)))
                                      .followed_by(Chomp0)
                                      .followed_by(Char(')', ")"))
                                      .parse(input));

        let (seed, remaining) = try_parse!(OptionalClause(Keyword("WITH").and_then(Chomp1)
                                                                         .and_then(Keyword("SEED"))
                                                                         .and_then(Chomp1)
                                                                         .and_then(PosIntLiteral)
                                                                         // TODO: checked cast
                                                                         .map(|seed| seed as u32)).parse(remaining));

        let (buckets, remaining) = try_parse!(Chomp1.and_then(Keyword("INTO"))
                                                    .and_then(Chomp1)
                                                    .and_then(I32)
                                                    .followed_by(Chomp1)
                                                    .followed_by(Keyword("BUCKETS"))
                                                    .parse(remaining));

        ParseResult::Ok(command::HashPartition::new(columns, seed, buckets), remaining)
    }
}

// WITH 1 REPLICA
// WITH 3 REPLICAS
struct Replicas;
impl <'a> Parser<'a> for Replicas {
    type Output = i32;
    fn parse(&self, input: &'a str) -> ParseResult<'a, i32> {
        Keyword("WITH").and_then(Chomp1)
                       .and_then(PosIntLiteral)
                       .map(|i| { i as i32 })
                       .followed_by(Chomp1)
                       .followed_by(OrElse(Keyword("REPLICAS"), Keyword("REPLICA")))
                       .parse(input)
    }
}

struct CreateTable;
impl <'a> Parser<'a> for CreateTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let (name, remaining) = try_parse!(
            Keyword("CREATE").and_then(Chomp1)
                             .and_then(Keyword("TABLE"))
                             .and_then(Chomp1)
                             .and_then(TableName)
                             .parse(input));

        let (columns, remaining) =
            try_parse!(Chomp1.and_then(Char('(', "("))
                                              .and_then(Delimited1(Chomp0.and_then(ColumnSpec),
                                                                   Chomp0.and_then(Char(',', ","))))
                                              .followed_by(Chomp0)
                                              .followed_by(Char(')', ")"))
                                              .parse(remaining));

        let (primary_key, remaining) =
            try_parse!(Chomp1.and_then(PrimaryKey).parse(remaining));

        let (distribute_by, remaining) =
            try_parse!(OptionalClause(Keyword("DISTRIBUTE").and_then(Chomp1)
                                                           .and_then(Keyword("BY")))
                       .parse(remaining));

        let (range_partition, hash_partitions, remaining) = if distribute_by.is_some() {
            let (range_partition, remaining) =
                try_parse!(OptionalClause(RangePartition).parse(remaining));
            let (hash_partitions, remaining) =
                try_parse!(Many0(Chomp1.and_then(HashPartition)).parse(remaining));
            (range_partition, hash_partitions, remaining)
        } else {
            (None, Vec::new(), remaining)
        };

        let (replicas, remaining) = try_parse!(OptionalClause(Replicas).parse(remaining));

        let (_, remaining) =
            try_parse!(Chomp0.and_then(Char(';', ";")).parse(remaining));

        ParseResult::Ok(command::Command::CreateTable {
            name: name,
            columns: columns,
            primary_key: primary_key,
            range_partition: range_partition,
            hash_partitions: hash_partitions,
            replicas: replicas,
        }, remaining)
    }
}

struct RenameTable;
impl <'a> Parser<'a> for RenameTable {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        Keyword("RENAME").and_then(Chomp1)
                         .and_then(Keyword("TO"))
                         .and_then(Chomp1)
                         .and_then(TableName)
                         .map(|table_name| command::AlterTableStep::RenameTable {
                             table_name: table_name
                         })
                         .parse(input)
    }
}

struct RenameColumn;
impl <'a> Parser<'a> for RenameColumn {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        let (old_column_name, remaining) =
            try_parse!(Keyword("RENAME").and_then(Chomp1)
                                        .and_then(Keyword("COLUMN"))
                                        .and_then(Chomp1)
                                        .and_then(ColumnName)
                                        .followed_by(Chomp1)
                                        .followed_by(Keyword("TO"))
                                        .followed_by(Chomp1)
                                        .and_then(ColumnName)
                                        .parse(input));
        let (new_column_name, remaining) = try_parse!(ColumnName.parse(remaining));
        ParseResult::Ok(command::AlterTableStep::RenameColumn {
            old_column_name: old_column_name,
            new_column_name: new_column_name,
        }, remaining)
    }
}

struct AddColumn;
impl <'a> Parser<'a> for AddColumn {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        Keyword("ADD").and_then(Chomp1)
                      .and_then(Keyword("COLUMN"))
                      .and_then(Chomp1)
                      .and_then(ColumnSpec)
                      .map(|column| command::AlterTableStep::AddColumn { column: column })
                      .parse(input)
    }
}

struct DropColumn;
impl <'a> Parser<'a> for DropColumn {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        Keyword("DROP").and_then(Chomp1)
                       .and_then(Keyword("COLUMN"))
                       .and_then(Chomp1)
                       .and_then(ColumnName)
                       .map(|column_name| command::AlterTableStep::DropColumn {
                           column_name: column_name
                       })
                       .parse(input)
    }
}

struct AddRangePartition;
impl <'a> Parser<'a> for AddRangePartition {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        Keyword("ADD").and_then(Chomp1)
                      .and_then(Keyword("RANGE"))
                      .and_then(Chomp1)
                      .and_then(Keyword("PARTITION"))
                      .and_then(Chomp1)
                      .and_then(RangeBound)
                      .map(|(lower_bound, upper_bound)| command::AlterTableStep::AddRangePartition {
                          lower_bound: lower_bound,
                          upper_bound: upper_bound,
                      })
                      .parse(input)
    }
}

struct DropRangePartition;
impl <'a> Parser<'a> for DropRangePartition {
    type Output = command::AlterTableStep<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::AlterTableStep<'a>> {
        Keyword("DROP").and_then(Chomp1)
                       .and_then(Keyword("RANGE"))
                       .and_then(Chomp1)
                       .and_then(Keyword("PARTITION"))
                       .and_then(Chomp1)
                       .and_then(RangeBound)
                       .map(|(lower_bound, upper_bound)| command::AlterTableStep::DropRangePartition {
                           lower_bound: lower_bound,
                           upper_bound: upper_bound,
                       })
                       .parse(input)
    }
}

struct AlterTable;
impl <'a> Parser<'a> for AlterTable {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let (table_name, remaining) = try_parse!(
            Keyword("ALTER").and_then(Chomp1)
                             .and_then(Keyword("TABLE"))
                             .and_then(Chomp1)
                             .and_then(TableName)
                             .followed_by(Chomp1)
                             .parse(input));

        let step = RenameTable.or_else(RenameColumn)
                              .or_else(AddColumn)
                              .or_else(DropColumn)
                              .or_else(AddRangePartition)
                              .or_else(DropRangePartition);

        let (steps, remaining) = try_parse!(
            Delimited1(step, Chomp0.and_then(Char(',', ",")).and_then(Chomp0)).parse(remaining));

        ParseResult::Ok(command::Command::AlterTable {
            table_name: table_name,
            steps: steps,
        }, remaining)
    }
}

// Parses ("abc", 123, 4.56)
struct Row;
impl <'a> Parser<'a> for Row {
    type Output = Vec<command::Literal<'a>>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, Vec<command::Literal<'a>>> {
        Char('(', "(").and_then(Chomp0)
                      .and_then(Delimited0(Literal, Chomp0.and_then(Char(',', ","))
                                                          .and_then(Chomp0)))
                      .followed_by(Chomp0)
                      .followed_by(Char(')', ")"))
                      .parse(input)
    }
}

struct Insert;
impl <'a> Parser<'a> for Insert {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let (table, remaining) = try_parse!(
            Keyword("INSERT").and_then(Chomp1)
                             .and_then(Keyword("INTO"))
                             .and_then(Chomp1)
                             .and_then(TableName)
                             .parse(input));

        let (columns, remaining) =
            try_parse!(OptionalClause(Char('(', "(").and_then(Delimited1(Chomp0.and_then(ColumnName),
                                                                         Chomp0.and_then(Char(',', ","))))
                                                    .followed_by(Chomp0)
                                                    .followed_by(Char(')', ")")))
                       .parse(remaining));

        let (rows, remaining) =
            try_parse!(Chomp1.and_then(Keyword("VALUES"))
                                              .and_then(Chomp0)
                                              .and_then(Delimited1(Row, Chomp0.and_then(Char(',', ","))
                                                                              .and_then(Chomp0)))
                                              .parse(remaining));

        let (_, remaining) = try_parse!(Chomp0.and_then(Char(';', ";")).parse(remaining));

        ParseResult::Ok(command::Command::Insert {
            table: table,
            columns: columns,
            rows: rows,
        }, remaining)
    }
}

// Parses '*', 'COUNT(*)' or 'col1, col2, ..'
struct Selector;
impl <'a> Parser<'a> for Selector {
    type Output = command::Selector<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Selector<'a>> {
        let star = Char('*', "*").map(|_| command::Selector::Star);
        let count_star = Keyword("COUNT").and_then(Chomp0)
                                         .and_then(Char('(', "("))
                                         .and_then(Chomp0)
                                         .and_then(Char('*', "*"))
                                         .and_then(Chomp0)
                                         .and_then(Char(')', ")"))
                                         .map(|_| command::Selector::CountStar);
        let columns = Delimited1(ColumnName, Chomp0.and_then(Char(',', ","))
                                                                    .and_then(Chomp0))
                        .map(|columns| command::Selector::Columns(columns));
        star.or_else(count_star.or_else(columns)).parse(input)
    }
}

// Parses:
//  SELECT * FROM foo;
//  SELECT a, b, c FROM foo;
//  SELECT COUNT(*) FROM foo;
struct Select;
impl <'a> Parser<'a> for Select {
    type Output = command::Command<'a>;
    fn parse(&self, input: &'a str) -> ParseResult<'a, command::Command<'a>> {
        let (selector, remaining) =
            try_parse!(Keyword("SELECT").and_then(Chomp1)
                                        .and_then(Selector)
                                        .parse(input));

        let (table, remaining) =
            try_parse!(Chomp1.and_then(Keyword("FROM"))
                                              .and_then(Chomp1)
                                              .and_then(TableName)
                                              .parse(remaining));

        let (_, remaining) =
            try_parse!(Chomp0.and_then(Char(';', ";")).parse(remaining));

        ParseResult::Ok(command::Command::Select {
            table: table,
            selector: selector,
        }, remaining)
    }
}

#[cfg(test)]
mod test {

    use std::borrow::Cow;

    use kudu;
    use chrono;

    use super::{
        BlockComment,
        BoolLiteral,
        Char,
        Chomp0,
        ColumnName,
        Command,
        ColumnSpec,
        CreateTable,
        DataType,
        Delimited1,
        DescribeTable,
        DoubleQuotedStringLiteral,
        FloatLiteral,
        HashPartition,
        HexLiteral,
        Hint,
        I32,
        Insert,
        IntLiteral,
        Keyword,
        LineComment,
        Literal,
        Noop,
        ParseResult,
        Parser,
        PosIntLiteral,
        RangePartition,
        Row,
        Select,
        ShowMasters,
        ShowTables,
        ShowTabletServers,
        TimestampLiteral,
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
    fn test_show_masters() {
        let parser = ShowTables;
        assert_eq!(parser.parse("SHOW MASTERS ;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
        assert_eq!(parser.parse("shOw MASTERS;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
        assert_eq!(parser.parse("shOw \t\r\nMASTERS;next command"),
                   ParseResult::Ok(command::Command::ShowTables, "next command"));

        assert_eq!(parser.parse("SHOW--mycoment ; foo bar\nMASTERS;"),
                   ParseResult::Ok(command::Command::ShowTables, ""));
    }

    #[test]
    fn test_show_tablet_servers() {
        let parser = ShowTabletServers;
        assert_eq!(parser.parse("SHOW TABLET SERVERS ;"),
                   ParseResult::Ok(command::Command::ShowTabletServers, ""));
        assert_eq!(parser.parse("shOw TABLET SERVERS;"),
                   ParseResult::Ok(command::Command::ShowTabletServers, ""));
        assert_eq!(parser.parse("shOw \t\r\nTABLET\t\r\nSERVERS;next command"),
                   ParseResult::Ok(command::Command::ShowTabletServers, "next command"));

        assert_eq!(parser.parse("SHOW--mycoment ; foo bar\nTABLET SERVERS;"),
                   ParseResult::Ok(command::Command::ShowTabletServers, ""));
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
        let parser = ColumnSpec;
        assert_eq!(parser.parse("foo int32"),
                   ParseResult::Ok(command::ColumnSpec::new("foo",
                                                            kudu::DataType::Int32,
                                                            None, None, None, None), ""));
        assert_eq!(parser.parse("foo int32 NULLABLE BLOCK SIZE 4096;"),
                   ParseResult::Ok(command::ColumnSpec::new("foo",
                                                            kudu::DataType::Int32,
                                                            Some(true),
                                                            None, None, Some(4096)), ";"));

        assert_eq!(parser.parse("foo timestamp NOT NULL ENCODING runlength COMPRESSION zlib BLOCK SIZE 99;"),
                   ParseResult::Ok(command::ColumnSpec::new("foo",
                                                            kudu::DataType::Timestamp,
                                                            Some(false),
                                                            Some(kudu::EncodingType::RunLength),
                                                            Some(kudu::CompressionType::Zlib),
                                                            Some(99)), ";"));
    }

    #[test]
    fn test_delimited() {
        let parser = Delimited1(Keyword("FUZZ"), Chomp0.followed_by(Char(',', ",")).followed_by(Chomp0));
        assert_eq!(parser.parse("fuzz, fuzz    -- sdf \n , fuzz,fuzz,fuzz;"),
                   ParseResult::Ok(vec!["fuzz", "fuzz", "fuzz", "fuzz", "fuzz"], ";"));
        assert_eq!(parser.parse("fuzz,; fuzz"),
                   error(Hint::Constant("FUZZ"), "; fuzz"));
        assert_eq!(parser.parse(",; fuzz"),
                   error(Hint::Constant("FUZZ"), ",; fuzz"));
    }

    #[test]
    fn test_create_table() {
        let parser = CreateTable;
        assert_eq!(parser.parse("create table t (a int32 not null, b timestamp, c string) primary key (a, c);"),
                   ParseResult::Ok(command::Command::CreateTable {
                       name: "t",
                       columns: vec![
                           command::ColumnSpec::new("a", kudu::DataType::Int32, Some(false), None, None, None),
                           command::ColumnSpec::new("b", kudu::DataType::Timestamp, None, None, None, None),
                           command::ColumnSpec::new("c", kudu::DataType::String, None, None, None, None),
                       ],
                       primary_key: vec!["a", "c"],
                       range_partition: None,
                       hash_partitions: Vec::new(),
                       replicas: None,
                   }, ""));

        assert_eq!(parser.parse("create table t (foo int32) primary key (foo) DISTRIBUTE BY RANGE (foo);"),
                   ParseResult::Ok(command::Command::CreateTable {
                       name: "t",
                       columns: vec![command::ColumnSpec::new("foo", kudu::DataType::Int32, None, None, None, None)],
                       primary_key: vec!["foo"],
                       range_partition: Some(command::RangePartition::new(vec!["foo"], vec![])),
                       hash_partitions: Vec::new(),
                       replicas: None,
                   }, ""));

        assert_eq!(parser.parse("create table t (foo int32) primary key (foo) DISTRIBUTE BY HASH (foo, bar) INTO 99 buckets;"),
                   ParseResult::Ok(command::Command::CreateTable {
                       name: "t",
                       columns: vec![command::ColumnSpec::new("foo", kudu::DataType::Int32, None, None, None, None)],
                       primary_key: vec!["foo"],
                       range_partition: None,
                       hash_partitions: vec![command::HashPartition::new(vec!["foo", "bar"], None, 99)],
                       replicas: None,
                   }, ""));

        assert_eq!(parser.parse("create table t (foo int32) \
                                primary key (foo) \
                                DISTRIBUTE BY \
                                    RANGE (a) SPLIT ROWS (1), (2) \
                                    HASH (b) INTO 99 buckets \
                                    hash (c) with seed 9 into 50 buckets \
                                WITH 10 REPLICAS;"),
                   ParseResult::Ok(command::Command::CreateTable {
                       name: "t",
                       columns: vec![command::ColumnSpec::new("foo", kudu::DataType::Int32, None, None, None, None)],
                       primary_key: vec!["foo"],
                       range_partition: Some(command::RangePartition::new(vec!["a"], vec![vec![command::Literal::Integer(1)],
                                                                                          vec![command::Literal::Integer(2)]])),
                       hash_partitions: vec![command::HashPartition::new(vec!["b"], None, 99),
                                             command::HashPartition::new(vec!["c"], Some(9), 50)],
                       replicas: Some(10),
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

    #[test]
    fn test_int_literal() {
        let parser = IntLiteral;
        assert_eq!(parser.parse("9"),
                   ParseResult::Ok(9, ""));
        assert_eq!(parser.parse("1234"),
                   ParseResult::Ok(1234, ""));
        assert_eq!(parser.parse("01234"),
                   ParseResult::Ok(1234, ""));
        assert_eq!(parser.parse("-01234"),
                   ParseResult::Ok(-1234, ""));
        assert_eq!(parser.parse("-01234.1234"),
                   ParseResult::Ok(-1234, ".1234"));
        assert_eq!(parser.parse("234xyz"),
                   ParseResult::Ok(234, "xyz"));

        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Integer, "")]));
        assert_eq!(parser.parse("-"),
                   ParseResult::Incomplete(vec![(Hint::Integer, "-")]));

        assert_eq!(parser.parse("f"),
                   ParseResult::Err(vec![Hint::Integer], "f"));
        assert_eq!(parser.parse("-f"),
                   ParseResult::Err(vec![Hint::Integer], "-f"));
    }

    #[test]
    fn test_pos_int_literal() {
        let parser = PosIntLiteral;
        assert_eq!(parser.parse("9"),
                   ParseResult::Ok(9, ""));
        assert_eq!(parser.parse("1234"),
                   ParseResult::Ok(1234, ""));
        assert_eq!(parser.parse("01234"),
                   ParseResult::Ok(1234, ""));
        assert_eq!(parser.parse("234xyz"),
                   ParseResult::Ok(234, "xyz"));

        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::PosInteger, "")]));

        assert_eq!(parser.parse("f"),
                   ParseResult::Err(vec![Hint::PosInteger], "f"));
        assert_eq!(parser.parse("-9"),
                   ParseResult::Err(vec![Hint::PosInteger], "-9"));
    }

    #[test]
    fn test_float_literal() {
        let parser = FloatLiteral;
        assert_eq!(parser.parse("9."),
                   ParseResult::Ok(9.0, ""));
        assert_eq!(parser.parse("9.0"),
                   ParseResult::Ok(9.0, ""));
        assert_eq!(parser.parse("01234.567"),
                   ParseResult::Ok(1234.567, ""));
        assert_eq!(parser.parse("234.5xyz"),
                   ParseResult::Ok(234.5, "xyz"));
        assert_eq!(parser.parse("-9.000"),
                   ParseResult::Ok(-9.0, ""));
        assert_eq!(parser.parse("-1234.e3"),
                   ParseResult::Ok(-1234000.0, ""));
        assert_eq!(parser.parse("-1234.0e-3"),
                   ParseResult::Ok(-1.234, ""));
        assert_eq!(parser.parse("-.e-3.xyz"),
                   ParseResult::Ok(0.0, ".xyz"));

        assert_eq!(parser.parse("9"),
                   ParseResult::Incomplete(vec![(Hint::Float, "9")]));
        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Float, "")]));

        assert_eq!(parser.parse("f"),
                   ParseResult::Err(vec![Hint::Float], "f"));
        assert_eq!(parser.parse("9f"),
                   ParseResult::Err(vec![Hint::Float], "9f"));
        assert_eq!(parser.parse("123.45ef"),
                   ParseResult::Err(vec![Hint::Float], "123.45ef"));
    }

    #[test]
    fn test_bool_literal() {
        let parser = BoolLiteral;
        assert_eq!(parser.parse("true"),
                   ParseResult::Ok(true, ""));
        assert_eq!(parser.parse("false"),
                   ParseResult::Ok(false, ""));

        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Constant("true"), ""),
                                                (Hint::Constant("false"), "")]));
        assert_eq!(parser.parse("tru"),
                   ParseResult::Incomplete(vec![(Hint::Constant("true"), "tru")]));
        assert_eq!(parser.parse("f"),
                   ParseResult::Incomplete(vec![(Hint::Constant("false"), "f")]));

        assert_eq!(parser.parse("x"),
                   ParseResult::Err(vec![Hint::Constant("true"), Hint::Constant("false")], "x"));
    }

    #[test]
    fn test_timestamp_literal() {
        let parser = TimestampLiteral;
        assert_eq!(parser.parse("1990-12-31T23:59:60Z"),
                   ParseResult::Ok(chrono::DateTime::parse_from_rfc3339("1990-12-31T23:59:60Z").unwrap(), ""));
        assert_eq!(parser.parse("1996-12-19T16:39:57-08:00"),
                   ParseResult::Ok(chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00").unwrap(), ""));
        assert_eq!(parser.parse("1996-12-19T16:39:57.1234-12:00"),
                   ParseResult::Ok(chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.1234-12:00").unwrap(), ""));

        assert_eq!(parser.parse("1996-12-19T16:39:57.1234Zoo.foo"),
                   ParseResult::Ok(chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.1234Z").unwrap(), "oo.foo"));

        assert_eq!(parser.parse(""),
                   ParseResult::Incomplete(vec![(Hint::Timestamp, "")]));
        assert_eq!(parser.parse("1990-"),
                   ParseResult::Incomplete(vec![(Hint::Timestamp, "1990-")]));

        assert_eq!(parser.parse("x"),
                   ParseResult::Err(vec![Hint::Timestamp], "x"));
        assert_eq!(parser.parse("1996-42-19T16:39:57Z"),
                   ParseResult::Err(vec![Hint::Timestamp], "1996-42-19T16:39:57Z"));
    }

    #[test]
    fn test_literal() {
        let parser = Literal;

        assert_eq!(parser.parse("1990-12-31T23:59:60Z"),
                   ParseResult::Ok(command::Literal::Timestamp(chrono::DateTime::parse_from_rfc3339("1990-12-31T23:59:60Z").unwrap()), ""));
        assert_eq!(parser.parse("1996"),
                   ParseResult::Ok(command::Literal::Integer(1996), ""));
        assert_eq!(parser.parse("1996.12"),
                   ParseResult::Ok(command::Literal::Float(1996.12), ""));
        assert_eq!(parser.parse("true"),
                   ParseResult::Ok(command::Literal::Bool(true), ""));
        assert_eq!(parser.parse("false"),
                   ParseResult::Ok(command::Literal::Bool(false), ""));
        assert_eq!(parser.parse("\"foobar\""),
                   ParseResult::Ok(command::Literal::String(Cow::Borrowed("foobar")), ""));
        assert_eq!(parser.parse("\"foo\0bar\""),
                   ParseResult::Ok(command::Literal::String(Cow::Owned("foo\0bar".to_string())), ""));
        assert_eq!(parser.parse("0x0102"),
                   ParseResult::Ok(command::Literal::Binary(vec![1, 2]), ""));
    }

    #[test]
    fn test_row() {
        let parser = Row;

        assert_eq!(parser.parse("(  1990-12-31T23:59:60Z, 1996,1996.12,true,     /* */ false,\"foobar\",  \"foo\0bar\",0x0102  )foo"),
                   ParseResult::Ok(vec![command::Literal::Timestamp(chrono::DateTime::parse_from_rfc3339("1990-12-31T23:59:60Z").unwrap()),
                                        command::Literal::Integer(1996),
                                        command::Literal::Float(1996.12),
                                        command::Literal::Bool(true),
                                        command::Literal::Bool(false),
                                        command::Literal::String(Cow::Borrowed("foobar")),
                                        command::Literal::String(Cow::Owned("foo\0bar".to_string())),
                                        command::Literal::Binary(vec![1, 2])],
                                    "foo"));
    }

    #[test]
    fn test_range_partition() {
        let parser = RangePartition;

        assert_eq!(parser.parse("RANGE(a, b, c) SPLIT ROWS (123), (123, \"foo\")"),
                   ParseResult::Ok(command::RangePartition::new(vec!["a", "b", "c"],
                                                                vec![vec![command::Literal::Integer(123)],
                                                                        vec![command::Literal::Integer(123),
                                                                            command::Literal::String(Cow::Borrowed("foo"))]]), ""));

        assert_eq!(parser.parse("RANGE(a, b, c)"),
                   ParseResult::Ok(command::RangePartition::new(vec!["a", "b", "c"], vec![]), ""));

    }

    #[test]
    fn test_hash_partition() {
        let parser = HashPartition;

        assert_eq!(parser.parse("HASH (a, b, c) WITH SEED 99 INTO 16 BUCKETS HASH"),
                   ParseResult::Ok(command::HashPartition::new(vec!["a", "b", "c"], Some(99), 16), " HASH"));

        assert_eq!(parser.parse("HASH (foo) INTO 99 BUCKETS HASH"),
                   ParseResult::Ok(command::HashPartition::new(vec!["foo"], None, 99), " HASH"));

        assert_eq!(parser.parse("HASH () INTO 99 BUCKETS HASH"),
                   ParseResult::Err(vec![Hint::Column("")], ") INTO 99 BUCKETS HASH"));
    }

    #[test]
    fn test_insert() {
        let parser = Insert;
        assert_eq!(parser.parse("insert into t values (1);"),
                   ParseResult::Ok(command::Command::Insert {
                       table: "t",
                       columns: None,
                       rows: vec![vec![command::Literal::Integer(1)]],
                   }, ""));

        assert_eq!(parser.parse("insert into t (foo, bar) values (1, 2);"),
                   ParseResult::Ok(command::Command::Insert {
                       table: "t",
                       columns: Some(vec!["foo", "bar"]),
                       rows: vec![vec![command::Literal::Integer(1), command::Literal::Integer(2)]],
                   }, ""));

        assert_eq!(parser.parse("insert into t values (1, 2), (99);"),
                   ParseResult::Ok(command::Command::Insert {
                       table: "t",
                       columns: None,
                       rows: vec![vec![command::Literal::Integer(1), command::Literal::Integer(2)],
                                  vec![command::Literal::Integer(99)]],
                   }, ""));
    }

    #[test]
    fn test_select() {
        let parser = Select;
        assert_eq!(parser.parse("select * from t;"),
                   ParseResult::Ok(command::Command::Select {
                       table: "t",
                       selector: command::Selector::Star,
                   }, ""));

    }
}
