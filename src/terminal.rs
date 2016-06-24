use std::cmp;
use std::io::Write;

use kudu;
use libc;
use term;

use parser::Hint;
use Color;
use HELP;

pub struct Terminal {
    out: Box<term::StdoutTerminal>,
    err: Box<term::StderrTerminal>,
    color: bool,
}

impl Terminal {

    /// Creates a new terminal.
    pub fn new(color: Color) -> Terminal {
        let color = match color {
            Color::Auto => unsafe { libc::isatty(libc::STDERR_FILENO) != 0 },
            Color::Always => true,
            Color::Never => false,
        };

        Terminal {
            out: term::stdout().expect("unable to open stdout"),
            err: term::stderr().expect("unable to open stderr"),
            color: color,
        }
    }

    /// Colors stderr red.
    fn red(&mut self) {
        if self.color {
            self.err.fg(term::color::BRIGHT_RED).unwrap();
        }
    }

    /// Style output to stderr as bold.
    fn bold(&mut self) {
        if self.color {
            self.err.attr(term::Attr::Bold).unwrap();
        }

    }

    /// Resets the style and color of stderr to the default.
    fn reset(&mut self) {
        if self.color {
            self.err.reset().unwrap();
        }
    }

    /// Prints a parse error to stderr.
    pub fn print_parse_error(&mut self, input: &str, remaining: &str, hints: &[Hint]) {
        let error_idx = input.len() - remaining.len();
        assert_eq!(&input[error_idx..], remaining);
        if !input.is_empty() {
            for line in input.lines() {

                if line.len() >= error_idx {
                    self.red();
                    write!(self.err, "error: ").unwrap();
                    self.reset();

                    let hints = hints.into_iter().map(|hint| {
                        match hint {
                            &Hint::Constant(ref expected) => format!("'{}'", *expected,),
                            &Hint::Integer => "an integer".to_string(),
                            &Hint::PosInteger => "a positive integer".to_string(),
                            &Hint::Float => "a floating point value".to_string(),
                            &Hint::Timestamp => "a timestamp value".to_string(),
                            &Hint::CharEscape => "a character escape sequence".to_string(),
                            &Hint::HexEscape => "a hex escape sequence".to_string(),
                            &Hint::Table(_) => "a table name".to_string(),
                            &Hint::Column(_) => "a column name".to_string(),
                        }
                    }).collect::<Vec<_>>();

                    self.bold();
                    writeln!(self.err, "expected: {}", hints.join(" or ")).unwrap();
                    self.reset();

                    let word_len = input[error_idx..].split_whitespace().next().unwrap_or("").len();
                    writeln!(self.err, "{}", line).unwrap();
                    writeln!(self.err, "{:>2$}{:~<3$}", "", "^", error_idx, word_len).unwrap();
                    break;
                }
            }
        }
        writeln!(&mut self.out, "").unwrap();
    }

    /// Prints a kudu err to stderr.
    pub fn print_kudu_error(&mut self, error: &kudu::Error) {
        self.red();
        write!(self.err, "error: ").unwrap();
        self.reset();
        writeln!(self.err, "{}", error).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_success(&mut self, msg: &str) {
        writeln!(self.out, "{}", msg).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_help(&mut self) {
        writeln!(&mut self.out, "{}", HELP).unwrap();
        writeln!(&mut self.out, "").unwrap();
    }

    /// Prints a table list to stdout.
    pub fn print_table_list(&mut self, tables: &[String]) {
        let width = cmp::max(5, tables.iter().map(|name| name.len()).max().unwrap_or(0));

        writeln!(&mut self.out, "{:<1$}", "Table", width).unwrap();
        writeln!(&mut self.out, "{:-^1$}", "", width).unwrap();
        for table in tables {
            writeln!(&mut self.out, "{:<1$}", table, width).unwrap();
        }
        writeln!(&mut self.out, "").unwrap();
    }

    /// Prints a table description to stdout.
    pub fn print_table_description(&mut self, schema: &kudu::Schema) {
        let columns = (0..schema.columns().len()).map(|idx| schema.column(idx).unwrap()).collect::<Vec<_>>();

        let mut names = vec!["Column".to_owned()];
        names.extend(columns.iter().map(|col| col.name().to_owned()));

        let mut types = vec!["Type".to_owned()];
        types.extend(columns.iter().map(|col| format!("{:?}", col.data_type())));

        let mut nullables = vec!["Nullable".to_owned()];
        nullables.extend(columns.iter().map(|col| {
            if col.is_nullable() { "True".to_owned() } else { "False".to_owned() }
        }));

        let mut encodings = vec!["Encoding".to_owned()];
        encodings.extend(columns.iter().map(|col| format!("{:?}", col.encoding())));

        let mut compressions = vec!["Compression".to_owned()];
        compressions.extend(columns.iter().map(|col| format!("{:?}", col.compression())));

        self.print_table(&[&names, &types, &nullables, &encodings, &compressions]);
    }

    pub fn print_table<C>(&mut self, columns: &[C]) where C: AsRef<[String]> {
        let rows = columns.iter().fold(None, |prev, col| {
            let col = col.as_ref();
            let rows = col.len();
            if let Some(prev_rows) = prev {
                assert_eq!(prev_rows, rows);
            }
            Some(rows)
        }).expect("there must be at least a header");
        let widths = columns.iter()
                            .map(|col| col.as_ref().iter().map(|cell| cell.len()).max().unwrap_or(0))
                            .collect::<Vec<_>>();

        for row in 0..rows {
            for col in 0..columns.len() {
                if col == 0 {
                    write!(&mut self.out, "{:<1$} ", columns[col].as_ref()[row], widths[col]).unwrap();
                } else if col == columns.len() - 1 {
                    write!(&mut self.out, "| {:<1$}", columns[col].as_ref()[row], widths[col]).unwrap();
                } else {
                    write!(&mut self.out, "| {:<1$} ", columns[col].as_ref()[row], widths[col]).unwrap();
                }
            }
            writeln!(&mut self.out, "").unwrap();

            if row == 0 {
                for col in 0..columns.len() {
                    if col == 0 {
                        write!(&mut self.out, "{:-<1$}", "", widths[col]).unwrap();
                    } else {
                        write!(&mut self.out, "-+-{:-<1$}", "", widths[col]).unwrap();
                    }
                }
                writeln!(&mut self.out, "").unwrap();
            }
        }
        writeln!(&mut self.out, "").unwrap();
    }

    pub fn print_not_implemented(&mut self) {
        writeln!(&mut self.out, "not yet implemented!").unwrap();
    }
}
