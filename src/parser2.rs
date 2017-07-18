

use parser::{Node};

macro_rules! tag (($name:ident, $tag:expr) => (
    let v = $tag;
    if let Err(_) = $name.consume(v) {
        let fail = $name.fail(MatchType::Tag(v));
        return fail;
    }
));

macro_rules! any_except (($name:ident, $chars:expr) => (
        {
            let v = $chars;
            match $name.consume_except(v) {
                Ok(cur) => cur,
                Err(_) => {
                    let fail = $name.fail(MatchType::AnyExcept(v));
                    return fail;
                }
            }
        }
));

macro_rules! many_n (($state:ident, $n:expr, $err:ident, $func:ident) => (
        {
            let mut items = vec![];
            loop {
                let result = $func($state);
                match result {
                    ParseResult::Error(..) => { return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            if items.len() < $n {
                let error = $state.error(ParseError::$err);
                return error;
            }
            items
        }
));
#[macro_export] macro_rules! many_0 (
    ($state:ident, $func:ident | $err:ident) => ( many_n!($state, 0, $err, $func); ),
    ($state:ident, $func:ident) => ( many_n!($state, 0, InvalidBlock, $func); )
);
#[macro_export] macro_rules! many_1 (($state:ident, $func:ident | $err:ident) => ( many_n!($state, 1, $err, $func); ));

macro_rules! result (($state:ident, $value:expr) => (
        {
            $state.pop();
            ParseResult::Ok($value)
        }
));

macro_rules! parser (($name:ident( $state:ident $(, $arg:ident : $type:ty)* ) -> $out:ty $body:block) => (
        pub fn $name<'a>($state:&mut ParseState<'a> $(, $arg:$type)*) -> ParseResult<'a, $out> {
            $state.mark("expr");
            $state.ignore_space(true);
            $body
        }
));



#[derive(Debug)]
pub enum ParseError {
    EmptySearch,
    EmptyUpdate,
    InvalidBlock,
}

#[derive(Debug)]
pub enum ParseResult<'a, T> {
    Ok(T),
    Error(FrozenParseState, ParseError),
    Fail(MatchType<'a>),
}

#[derive(Debug)]
pub enum MatchType<'a> {
    Tag(&'a str),
    AnyExcept(&'a str),
}

#[derive(Clone, Debug)]
pub struct FrozenParseState {
    stack: Vec<(String, usize, usize, usize, bool)>,
    line: usize,
    ch: usize,
    pos: usize,
    ignore_space: bool,
}

#[derive(Clone, Debug)]
pub struct ParseState<'a> {
    input: &'a str,
    stack: Vec<(&'a str, usize, usize, usize, bool)>,
    line: usize,
    ch: usize,
    pos: usize,
    ignore_space: bool,
}

impl<'a> ParseState<'a> {
    pub fn new(input:&str) -> ParseState {
        ParseState { input, stack:vec![], line:0, ch:0, pos:0, ignore_space: false }
    }

    pub fn mark(&mut self, frame:&'a str) {
        self.stack.push((frame, self.line, self.ch, self.pos, self.ignore_space));
    }

    pub fn pop(&mut self) {
        let (_, _, _, _, ignore_space) = self.stack.pop().unwrap();
        self.ignore_space = ignore_space;
    }

    pub fn backtrack(&mut self) {
        let (_, line, ch, pos, ignore_space) = self.stack.pop().unwrap();
        self.ignore_space = ignore_space;
        self.line = line;
        self.ch = ch;
        self.pos = pos;
    }

    pub fn ignore_space(&mut self, ignore:bool) {
        self.ignore_space = ignore;
    }

    pub fn eat_space(&mut self) {
        let remaining = &self.input[self.pos..];
        for c in remaining.chars() {
            match c {
                ' ' | '\t' | ',' => { self.ch += 1; self.pos += 1; }
                '\n' => { self.line += 1; self.ch = 0; self.pos += 1; }
                '\r' => { }
                _ => { break }
            }
        }
    }

    pub fn consume_except(&mut self, chars:&str) -> Result<&'a str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        let start = self.pos;
        'outer: for c in remaining.chars() {
            for bad in chars.chars() {
                if c == bad { break 'outer; }
            }
            self.ch += 1;
            self.pos += 1;
        }
        Ok(&self.input[start..self.pos])
    }

    pub fn consume<'b>(&mut self, token:&'b str) -> Result<&'b str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        let token_len = token.len();
        if remaining.len() < token_len {
            return Err(());
        }
        for (a, b) in remaining.chars().zip(token.chars()) {
            if a != b {
                return Err(());
            }
        }
        self.ch += token_len;
        self.pos += token_len;
        Ok(token)
    }

    pub fn fail<'b, T>(&mut self, with:MatchType<'b>) -> ParseResult<'b, T> {
        self.backtrack();
        ParseResult::Fail(with)
    }

    pub fn error<'b, T>(&self, with:ParseError) -> ParseResult<'b, T> {
        ParseResult::Error(self.freeze(), with)
    }

    pub fn freeze(&self) -> FrozenParseState {
        let frozen_stack = self.stack.iter().map(|&(a,b,c,d,e)| (a.to_string(), b, c, d, e)).collect();
        FrozenParseState { pos:self.pos, ignore_space:self.ignore_space, stack: frozen_stack, line:self.line, ch:self.ch }
    }
}


parser!(identifier(state) -> Node<'a> {
    let v = any_except!(state, "#\\.,()[]{}:=\"|; \r\n\t");
    result!(state, Node::Variable(v))
});

parser!(expr(state) -> Node<'a> {
    tag!(state, "1");
    result!(state, Node::Variable("1"))
});

parser!(project_section(state) -> Node<'a> {
    tag!(state, "project");
    tag!(state, "(");
    let items = many_1!(state, expr | EmptyUpdate);
    tag!(state, ")");
    result!(state, Node::Project(items))
});

// parser!(alt_test(state) -> Node<'a> {
//     let v = alt!(state, [ project_section, expr, identifier ] | InvalidBlock);
//     result!(state, v)
// });

// parser!(value(state) -> Node<'a> {
//     let v = alt!(state, {
//         number,
//         string,
//         record_function,
//         record_reference,
//         wrapped_expression
//     });
//     result!(state, v)
// });

// parser!(project_section(state) -> Node<'a>, {
//     tag!("project");
//     tag!("(");
//     let items = many1!(EmptyUpdate, {
//         expr(state)
//     });
//     tag!(")");
//     result!(Node::Project(items));
// }

// parser!(project_section, {
//    project ( items:expr!+ )
// }, {
//     Node::Project(items)
// })

// parser!(block, {
//    search: search!?
//    update: { bind_section! | commit_section! | project_section! | watch_section! }
//    end
// }, {
//     Node::Block { search:Box::new(search), update:Box::new(update) }
// })

// pub fn project_section(state) -> Result<Node<'a>> {
//     state.mark("project_section");
//     state.ignore_space();
//     tag!(state, "project");
//     tag!(state, "(");
//     let items = many1!(state, EmptyUpdate, {
//         expr(state)
//     });
//     tag!(state, ")");
//     Node::Project(items)
// }
