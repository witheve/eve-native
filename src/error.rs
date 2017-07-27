extern crate term_painter;

use combinators::{Span, ParseResult, Pos};
use compiler::{Node};
use std::fmt;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;

#[derive(Debug, Clone, Copy)]
pub enum ParseError {
    EmptySearch,
    EmptyUpdate,
    InvalidBlock,
    MissingEnd,
    MissingUpdate,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ParseError::EmptySearch => { write!(f, "This block has an empty search. If you want a block to run\n unconditionally, you can omit the search section.") }
            &ParseError::EmptyUpdate => { write!(f, "This block doesn't have any actions in it.") }
            &ParseError::InvalidBlock => { write!(f, "This block is invalid, but unfortunately I don't have a lot of information about why.") }
            &ParseError::MissingEnd => { write!(f, "The `end` keyword is missing for this block.") }
            &ParseError::MissingUpdate => { write!(f, "This block is missing either a `bind` or `commit` section.") }
        }
    }
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::Unprovided(ref var) => { write!(f, "Nothing in the block is providing `{}`. You can search for\n something that provides `{}`, or bind a constant.\n e.g. `{}: \"Hello\"`", var, var, var) }
            &Error::UnknownFunction(ref func) => { write!(f, "I don't know the `{}` function, so I'm not sure what to execute.", func) }
            &Error::UnknownFunctionParam(ref func, ref param) => { write!(f, "The `{}` function doesn't have a `{}` attribute.", func, param) }
            &Error::ParseError(ref err) => { write!(f, "{}", err) }
        }
    }
}


fn format_error_source(span:&Span, lines:&Vec<&str>) {
    let start = &span.start;
    let stop = &span.stop;
    let start_line = start.line;
    let stop_line = stop.line;
    let mut line_marker = String::new();
    for line_ix in start_line..stop_line+1 {
        line_marker.push_str(&format!(" {}| ", line_ix + 1));
        print!("{}", BrightYellow.paint(&line_marker[..]));
        print!("{}",lines[line_ix]);
        print!("\n");
        
    }
    if span.single_line() {
        for _ in 0..line_marker.len() - 1 { print!(" "); }
        for _ in 0..start.ch + 1 { print!(" "); }
        print!("{}",BrightRed.paint("^"));
        for _ in 0..(stop.ch - start.ch - 1) { print!("{}", BrightRed.paint("-")); }
        print!("\n");
    }
}

pub fn from_parse_error<'a>(error: &ParseResult<Node<'a>>) -> CompileError {
    match error {
        &ParseResult::Error(ref info, err) => {
            let start = Pos { line:info.line, ch:info.ch, pos:info.pos };
            let mut stop = start.clone();
            stop.ch += 1;
            stop.pos += 1;
            CompileError { span: Span {start, stop} , error: Error::ParseError(err) }
        }
        _ => { panic!("Passed non-parse error to from_parse_error"); }
    }

}

pub fn report_errors(errors: &Vec<CompileError>, path:&str, source:&str) {
    let lines:Vec<&str> = source.split("\n").collect();
    let open = format!("\n----------------------------------------- {}\n", path);
    let close = "-".repeat(open.len() - 2);
    println!("{}", BrightCyan.paint(&open));
    for error in errors {
        println!(" {}\n", error.error);
        format_error_source(&error.span, &lines);
        println!("{}\n", BrightCyan.paint(&close));
    }
}
