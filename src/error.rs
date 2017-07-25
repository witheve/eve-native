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

pub fn from_parse_error<'a>(error: &ParseResult<Node<'a>>) -> CompileError {
    match error {
        &ParseResult::Error(ref info, err) => {
            let pos = Pos { line:info.line, ch:info.ch, pos:info.pos };
            CompileError { span: Span {start: pos.clone(), stop: pos} , error: Error::ParseError(err) }
        }
        _ => { panic!("Passed non-parse error to from_parse_error"); }
    }

}

pub fn report_errors(errors: &Vec<CompileError>) {
    println!("ERRORS {:?}", errors);
}
