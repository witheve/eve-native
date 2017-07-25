use combinators::{Span, ParseResult, Pos};
use compiler::{Node};
use std::fmt;

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
            &ParseError::EmptySearch => { write!(f, "Looks like this block has an empty search.") }
            &ParseError::EmptyUpdate => { write!(f, "Looks like this block doesn't have any actions in it.") }
            &ParseError::InvalidBlock => { write!(f, "This block is invalid, but unfortunately I don't have a lot of information about why.") }
            &ParseError::MissingEnd => { write!(f, "Looks like the end keyword is missing for this block.") }
            &ParseError::MissingUpdate => { write!(f, "Looks like this block doesn't have an action section.") }
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
            &Error::Unprovided(ref var) => { write!(f, "Looks like nothing in the block is providing `{}`", var) }
            &Error::UnknownFunction(ref func) => { write!(f, "I don't know the `{}` function, so I'm not sure what to execute.", func) }
            &Error::UnknownFunctionParam(ref func, ref param) => { write!(f, "The `{}` function doesn't seem to have a `{}` attribute.", func, param) }
            &Error::ParseError(ref err) => { write!(f, "{}", err) }
        }
    }
}

#[derive(Debug,PartialEq, Eq)]
pub enum Color {
    Normal,
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
}

impl fmt::Display for Color {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let color_code = match self {
            &Color::Normal => "0",
            &Color::Black => "30",
            &Color::Red => "31",
            &Color::Green => "32",
            &Color::Yellow => "33",
            &Color::Blue => "34",
            &Color::Magenta => "35",
            &Color::Cyan => "36",
            &Color::White => "37",
        };
        write!(f, "\x1B[{}m", color_code)
    }
}

fn format_error_source(span:&Span, lines:&Vec<&str>) -> String {
    let start = &span.start;
    let stop = &span.stop;
    let mut part = {
        let start_line = start.line;
        let stop_line = stop.line;
        let mut parts = String::new();
        for line_ix in start_line..stop_line+1 {
            parts.push_str(&format!("{}{}|{} ", Color::Yellow, line_ix + 1, Color::Normal));
            parts.push_str(lines[line_ix]);
            parts.push_str("\n");
        }
        parts.pop();
        parts
    };
    if span.single_line() {
        part.push_str(&format!("{}", Color::Red));
        part.push_str("\n   ");
        for _ in 0..start.ch + 1 { part.push_str(" "); }
        part.push_str("^");
        for _ in 0..(stop.ch - start.ch - 1) { part.push_str("-"); }
        part.push_str(&format!("{}", Color::Normal));
    }
    part
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
    let mut final_open_len = 0;
    for error in errors {
        let error_source = format_error_source(&error.span, &lines);
        let open = format!("\n-- ERROR -------------------------------- {}\n", path);
        println!("{}{}{}", Color::Cyan, open, Color::Normal);
        println!("{}\n", error.error);
        println!("{}", error_source);
        final_open_len = open.len();
    }
    let close = "-".repeat(final_open_len - 1);
    println!("\n{}{}{}\n", Color::Cyan, close, Color::Normal);
}
