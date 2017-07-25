use combinators::{Span, ParseResult, Pos};
use compiler::{Node};

#[derive(Debug, Clone, Copy)]
pub enum ParseError {
    EmptySearch,
    EmptyUpdate,
    InvalidBlock,
    MissingEnd,
    MissingUpdate,
}

#[derive(Debug, Clone)]
pub struct CompileError {
    pub span: Span,
    pub error: Error
}

#[derive(Debug, Clone)]
pub enum Error {
    Unprovided(String),
    UnknownFunction(String),
    UnknownFunctionParam(String, String),
    ParseError(ParseError),
}

fn format_error_source(span:&Span, lines:&Vec<&str>) -> String {
    let start = &span.start;
    let stop = &span.stop;
    let mut part = {
        let start_line = start.line;
        let stop_line = stop.line;
        let mut parts = String::new();
        for line_ix in start_line..stop_line+1 {
            parts.push_str(&format!("{}| ", line_ix + 1));
            parts.push_str(lines[line_ix]);
            parts.push_str("\n");
        }
        parts.pop();
        parts
    };
    if span.single_line() {
        part.push_str("\n   ");
        for _ in 0..start.ch { part.push_str(" "); }
        part.push_str("^");
        for _ in start.ch..stop.ch - 1 { part.push_str("-"); }
    }
    part
}

pub fn from_parse_error<'a>(error: &ParseResult<Node<'a>>) -> CompileError {
    match error {
        &ParseResult::Error(ref info, err) => {
            let pos = Pos { line:info.line, ch:info.ch, pos:info.pos };
            CompileError { span: Span {start: pos.clone(), stop: pos} , error: Error::ParseError(err) }
        }
        _ => { panic!("Passed non-parse error to from_parse_error"); }
    }

}

pub fn report_errors(errors: &Vec<CompileError>, path:&str, source:&str) {
    let lines:Vec<&str> = source.split("\n").collect();
    for error in errors {
        let error_source = format_error_source(&error.span, &lines);
        let open = format!("\n-- ERROR -------------------------------- {}\n", path);
        println!("{}", open);
        println!("{:?}\n", error.error);
        println!("{}", error_source);

        let mut close = String::with_capacity(open.len());
        for _ in 0..open.len() - 1 { close.push_str("-"); }
        println!("\n{}", close);

    }
}
