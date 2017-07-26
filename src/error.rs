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
        let open = format!("\n-- ERROR -------------------------------- {}\n", path);
        println!("{}", BrightCyan.paint(&open));
        println!("{}\n", error.error);
        let start = &error.span.start;
        let stop = &error.span.stop;
        let mut part = {
            let start_line = start.line;
            let stop_line = stop.line;
            let mut parts = String::new();
            for line_ix in start_line..stop_line+1 {
                println!("{}{}{}", BrightYellow.paint(line_ix + 1),BrightYellow.paint("|"),lines[line_ix]);
                parts.push_str(&format!("{}{}{}", BrightYellow.paint(line_ix + 1),BrightYellow.paint("|"),lines[line_ix]));
            }
            parts.pop();
            parts
        };
        if error.span.single_line() {
            for _ in 0..start.ch { print!(" "); }
            print!("{}",BrightRed.paint("  ^"));
            for _ in start.ch..stop.ch - 1 { part.push_str("-"); }
        }
        let close = "-".repeat(open.len() - 1);
        println!("\n{}", BrightCyan.paint(close));

    }
}
