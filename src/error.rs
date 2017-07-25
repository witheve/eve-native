
#[derive(Debug, Clone)]
pub enum ParseError {
    EmptySearch,
    EmptyUpdate,
    InvalidBlock,
    MissingEnd,
    MissingUpdate,
}

