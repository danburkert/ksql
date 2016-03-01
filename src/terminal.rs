use std::cmp;
use std::io::{self, Write};

use kudu;
use libc;
use term;

use parser::Hint;
use Color;

pub struct Terminal {
    out: Box<term::StdoutTerminal>,
    err: Box<term::StderrTerminal>,
    color_stdout: bool,
    color_stderr: bool,
}


fn stderr_isatty() -> bool {
    use libc;
    unsafe { libc::isatty(libc::STDERR_FILENO) != 0 }
}


impl Terminal {




    pub fn new(color: Color) -> Terminal {
        let (color_stdout, color_stderr) = match color {
            Color::Auto => unsafe { (libc::isatty(libc::STDOUT_FILENO) != 0,
                                     libc::isatty(libc::STDERR_FILENO) != 0) },
            Color::Always => (true, true),
            Color::Never => (false, false),
        };

        Terminal {
            out: term::stdout().expect("unable to open stdout"),
            err: term::stderr().expect("unable to open stderr"),
            color_stdout: color_stdout,
            color_stderr: color_stderr,
        }
    }

    fn stderr_red(&mut self) {
        if self.color_stderr {
            self.err.fg(term::color::BRIGHT_RED);
        }
    }

    fn stderr_bold(&mut self) {
        if self.color_stderr {
            self.err.attr(term::Attr::Bold);
        }

    }

    fn stderr_reset(&mut self) {
        if self.color_stderr {
            self.err.reset();
        }
    }

    /// Prints a parse error to the terminal.
    pub fn print_parse_error(&mut self, input: &str, remaining: &str, hints: &[Hint]) {
        let error_idx = input.len() - remaining.len();
        assert_eq!(&input[error_idx..], remaining);
        if !input.is_empty() {
            let mut line_start = 0;
            for (line_num, line) in input.lines().enumerate() {

                if line_start + line.len() >= error_idx {
                    let column = error_idx - line_start;

                    self.stderr_red();
                    write!(self.err, "error: ");
                    self.stderr_reset();

                    let hints = hints.into_iter().map(|hint| {
                        match hint {
                            &Hint::Constant(ref expected) => format!("'{}'", *expected,),
                            &Hint::Table(_) => "a table name".to_string(),
                        }
                    }).collect::<Vec<_>>();

                    self.stderr_bold();
                    writeln!(self.err, "expected: {}", hints.join(" or ")).unwrap();
                    self.stderr_reset();

                    let word_len = input[column..].split_whitespace().next().unwrap_or("").len();
                    writeln!(self.err, "{}", line).unwrap();
                    writeln!(self.err, "{:>2$}{:~<3$}", "", "^", column, word_len).unwrap();
                    break;
                }
            }
        }
    }

    pub fn print_kudu_error(&mut self, error: &kudu::Error) {
        self.stderr_red();
        write!(self.err, "error: ");
        self.stderr_reset();
        writeln!(self.err, "{}", error.message()).unwrap();
    }

    pub fn print_tables(&mut self, tables: &[&str]) {
        let width = cmp::max(5, tables.iter().map(|name| name.len()).max().unwrap_or(0));

        writeln!(&mut self.out, "{:<1$}", "Table", width).unwrap();
        writeln!(&mut self.out, "{:-^1$}", "", width).unwrap();
        for table in tables {
            writeln!(&mut self.out, "{:<1$}", table, width).unwrap();
        }
    }

    pub fn print_table(&mut self, schema: &kudu::Schema) {
        let num_columns = schema.num_columns();
        let num_primary_key_columns = schema.num_primary_key_columns();

        let columns = (0..num_columns).map(|idx| schema.column(idx)).collect::<Vec<_>>();
        let column_width = cmp::max(6, columns.iter().map(|col| col.name().len()).max().unwrap_or(0));

        fn type_width(ty: kudu::DataType) -> usize {
            match ty {
                kudu::DataType::Int8 => 4,
                kudu::DataType::Int16 => 5,
                kudu::DataType::Int32 => 5,
                kudu::DataType::Int64 => 5,
                kudu::DataType::String => 6,
                kudu::DataType::Bool => 4,
                kudu::DataType::Float => 5,
                kudu::DataType::Double => 6,
                kudu::DataType::Binary => 6,
                kudu::DataType::Timestamp => 9,
            }
        }

        let type_width = cmp::max(4, columns.iter().map(|col| type_width(col.data_type())).max().unwrap_or(0));

        writeln!(&mut self.out, " {:<3$} | {:<4$} | {}",
                 "Column", "Type", "Nullable", column_width, type_width).unwrap();
        writeln!(&mut self.out, "-{:-^2$}-+-{:-^3$}-+--------",
                 "", "", column_width, type_width).unwrap();
        for column in &columns {
            writeln!(&mut self.out, " {:<3$} | {:<4$} | {}",
                     column.name(),
                     format!("{:?}", column.data_type()),
                     if column.is_nullable() { "True" } else { "False" },
                     column_width,
                     type_width).unwrap();
        }
        writeln!(&mut self.out, "").unwrap();
        writeln!(&mut self.out, "PRIMARY KEY ({})",
                 columns.iter()
                        .take(schema.num_primary_key_columns())
                        .map(|col| col.name())
                        .collect::<Vec<_>>()
                        .join(", ")).unwrap();
    }
}

#[cfg(test)]
mod test {

    use parser::{
        Command,
        Hint,
        Parser,
        ParseResult,
    };

    use super::write_parse_error;

    /*
    #[test]
    fn test_write_parse_error() {
        fn test(input: &str, msg: &str) {
            if let ParseResult::Err(expected, remaining) = Command.parse(input) {
                let mut actual = Vec::new();
                write_parse_error(&mut actual, input, remaining, expected);
                assert_eq!(msg.as_bytes(), &actual[..]);
            } else {
                panic!("expected parse to fail")
            }
        }

        test("DESCRIBE foo;",
             "Error: expected TABLE\nDESCRIBE foo;\n         ^");
    }
    */
}
