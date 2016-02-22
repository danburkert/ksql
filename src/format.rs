use std::io;

/// Formats a parse error into an error string appropriate to show to users.
///
/// TODO: add some sweet colorized output.
pub fn write_parse_error<W>(w: &mut W, input: &str, remaining: &str, expected: &str)
where W: io::Write {
    let error_idx = input.len() - remaining.len();
    assert_eq!(&input[error_idx..], remaining);

    let mut line_start = 0;
    for line in input.lines() {
        if line_start + line.len() > error_idx {
            let offset = error_idx - line_start;

            if expected.is_empty() {
                writeln!(w, "Error: unrecognized input.");
            } else {
                writeln!(w, "Error: unrecognized input. Expected: '{}'", expected);
            }

            writeln!(w, "{}", line);
            writeln!(w, "{:>1$}^", "", offset);
            return;
        }
    }
    unreachable!()
}

#[cfg(test)]
mod test {

    use parser::{
        Command,
        Hint,
        Parser,
        ParseResult,
    };

    use super::format_error;

    #[test]
    fn test_format_error() {
        fn test(input: &str, msg: &str) {
            if let ParseResult::Err(expected, remaining) = Command.parse(input) {
                let mut actual = String::new();
                format_error(&mut actual, input, remaining, expected);
                assert_eq!(msg, actual);
            } else {
                panic!("expected parse to fail")
            }
        }

        test("DESCRIBE foo;",
             "Error: expected TABLE\nDESCRIBE foo;\n         ^");
    }
}
