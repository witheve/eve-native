use super::compiler::OutputType;

//--------------------------------------------------------------------
// Combinator Macros
//--------------------------------------------------------------------

#[macro_export]
macro_rules! call (
    ($state:ident, $func:ident) => ({
        let result = $func($state);
        match result {
            ParseResult::Ok(value) => { value }
            ParseResult::Fail(f) => { return $state.fail(f) }
            _ => { $state.pop(); return result },
        }
    });
    ($state:ident, $func:ident | $err:ident) => ({
        let result = $func($state);
        match result {
            ParseResult::Ok(value) => { value }
            _ => return $state.error(ParseError::$err);
        }
    });
);

#[macro_export]
macro_rules! opt (
    ($state:ident, $func:ident) => ({
        let result = $func($state);
        match result {
            ParseResult::Ok(value) => { Some(value) }
            _ => None,
        }
    });
);

#[macro_export]
macro_rules! tag (
    ($name:ident, $tag:expr) => (
        let v = $tag;
        if let Err(_) = $name.consume(v) {
            return $name.fail(MatchType::Tag(v));
        });
    ($name:ident, $tag:expr => $err:ident) => (
        let v = $tag;
        if let Err(_) = $name.consume(v) {
            return $name.error(ParseError::$err);
        });
);

#[macro_export]
macro_rules! any_except (($name:ident, $chars:expr) => (
        {
            let v = $chars;
            match $name.consume_except(v) {
                Ok(cur) => cur,
                Err(_) => {
                    return $name.fail(MatchType::AnyExcept(v));
                }
            }
        }
));

#[macro_export]
macro_rules! any (($name:ident, $chars:expr) => (
        {
            let v = $chars;
            match $name.consume_chars(v) {
                Ok(cur) => cur,
                Err(_) => { "" }
            }
        }
));

#[macro_export]
macro_rules! take_while (
    ($name:ident, $chars:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => cur,
                Err(_) => {
                    return $name.fail(MatchType::TakeWhile);
                }
            }
        });
    ($name:ident, $chars:ident => $err:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => { cur },
                Err(_) => {
                    return $name.error(ParseError::$err);
                }
            }
        });
);

#[macro_export]
macro_rules! take_while_1 (
    ($name:ident, $chars:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => { cur },
                Err(_) => {
                    return $name.fail(MatchType::TakeWhile);
                }
            }
        });
    ($name:ident, $chars:ident => $err:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => { cur },
                Err(_) => {
                    return $name.error(ParseError::$err);
                }
            }
        });
);


#[macro_export]
macro_rules! many_n (
    ($state:ident, $n:expr, $err:ident, $func:ident) => ({
            let mut items = vec![];
            loop {
                let result = $func($state);
                match result {
                    ParseResult::Error(..) => { $state.pop(); return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            if items.len() < $n {
                let error = $state.error(ParseError::$err);
                return error;
            }
            items
        });
    ($state:ident, $n:expr, $func:ident) => ({
            let mut items = vec![];
            loop {
                let result = $func($state);
                match result {
                    ParseResult::Error(..) => { $state.pop(); return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            if items.len() < $n {
                let error = $state.fail(MatchType::Many($n));
                return error;
            }
            items
        });
    ($state:ident, $func:ident) => ({
            let mut items = vec![];
            loop {
                let result = $func($state);
                match result {
                    ParseResult::Error(..) => { $state.pop(); return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            items
        });
);
#[macro_export] macro_rules! many (($state:ident, $func:ident) => ( many_n!($state, $func); ); );
#[macro_export] macro_rules! many_1 (
    ($state:ident, $func:ident => $err:ident) => ( many_n!($state, 1, $err, $func); );
    ($state:ident, $func:ident) => ( many_n!($state, 1, $func); );
);

#[macro_export]
macro_rules! alt_branch (
    ($state:ident, $cur:ident ) => ({ $cur($state) });
    ($state:ident, $cur:ident $(, $rest:ident)*) => ({
        let a = $cur($state);
        match a {
            ParseResult::Ok(..) => { a }
            ParseResult::Error(..) => { a }
            ParseResult::Fail(..) => {
                alt_branch!($state $(, $rest)*)
            }
        }
    });
);

#[macro_export]
macro_rules! alt (
    ($state:ident, [ $($rest:ident)* ] => $err:ident) => (
        {
            let result = alt_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return $state.error(ParseError::$err); }
                ParseResult::Error(frozen, info) => { $state.pop(); return ParseResult::Error(frozen, info); }
                ParseResult::Ok(v) => v
            }
        });
    ($state:ident, [ $($rest:ident)* ]) => (
        {
            let result = alt_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return $state.fail(MatchType::Alternative) }
                ParseResult::Error(frozen, info) => { $state.pop(); return ParseResult::Error(frozen, info); }
                ParseResult::Ok(v) => v
            }
        });
);

#[macro_export]
macro_rules! alt_tag_branch (
    ($state:ident, $cur:expr ) => ({
        let v = $cur;
        match $state.consume(v) {
            Ok(res) => ParseResult::Ok(res),
            Err(_) => ParseResult::Fail(MatchType::Alternative),
        }
    });
    ($state:ident, $cur:expr $(, $rest:expr)*) => ({
        let v = $cur;
        match $state.consume(v) {
            Ok(res) => ParseResult::Ok(res),
            Err(_) => {
                alt_tag_branch!($state $(, $rest)*)
            }
        }
    });
);

#[macro_export]
macro_rules! alt_tag (
    ($state:ident, [ $($rest:expr)* ] => $err:ident) => (
        {
            let result:ParseResult<&str> = alt_tag_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return $state.error(ParseError::$err); }
                ParseResult::Error(..) => { unreachable!() }
                ParseResult::Ok(v) => v
            }
        });
    ($state:ident, [ $($rest:expr)* ]) => (
        {
            let result:ParseResult<&str> = alt_tag_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return $state.fail(MatchType::Alternative); }
                ParseResult::Error(..) => { unreachable!() }
                ParseResult::Ok(v) => v
            }
        });
);

#[macro_export]
macro_rules! result (($state:ident, $value:expr) => (
        {
            $state.pop();
            ParseResult::Ok($value)
        }
));

#[macro_export]
macro_rules! parser (($name:ident( $state:ident $(, $arg:ident : $type:ty)* ) -> $out:ty $body:block) => (
        pub fn $name<'a>($state:&mut ParseState<'a> $(, $arg:$type)*) -> ParseResult<'a, $out> {
            $state.mark(stringify!($name));
            $state.ignore_space(true);
            $body
        }
));

#[macro_export]
macro_rules! whitespace_parser (($name:ident( $state:ident $(, $arg:ident : $type:ty)* ) -> $out:ty $body:block) => (
        pub fn $name<'a>($state:&mut ParseState<'a> $(, $arg:$type)*) -> ParseResult<'a, $out> {
            $state.mark(stringify!($name));
            $state.ignore_space(false);
            $body
        }
));

//--------------------------------------------------------------------
// Parse Result and Errors
//--------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ParseError {
    EmptySearch,
    EmptyUpdate,
    InvalidBlock,
    MissingEnd,
    MissingUpdate,
}

#[derive(Debug, Clone)]
pub enum ParseResult<'a, T> {
    Ok(T),
    Error(FrozenParseState, ParseError),
    Fail(MatchType<'a>),
}

#[derive(Debug, Clone)]
pub enum MatchType<'a> {
    Block,
    Alternative,
    TakeWhile,
    ConsumeUntil,
    Tag(&'a str),
    AnyExcept(&'a str),
    Many(usize),
}

//--------------------------------------------------------------------
// Parse State
//--------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct FrozenParseState {
    pub stack: Vec<(String, usize, usize, usize, bool)>,
    pub line: usize,
    pub ch: usize,
    pub pos: usize,
    pub ignore_space: bool,
}

#[derive(Clone, Debug)]
pub struct ParseState<'a> {
    pub input: &'a str,
    stack: Vec<(&'a str, usize, usize, usize, bool)>,
    capture_stack: Vec<usize>,
    pub line: usize,
    pub ch: usize,
    pub pos: usize,
    ignore_space: bool,
    pub output_type: OutputType,
}

impl<'a> ParseState<'a> {
    pub fn new(input:&str) -> ParseState {
        ParseState { input, stack:vec![], capture_stack:vec![], line:0, ch:0, pos:0, output_type: OutputType::Lookup, ignore_space: false }
    }

    pub fn capture(&mut self) {
        self.capture_stack.push(self.pos);
    }

    pub fn stop_capture(&mut self) -> &'a str {
        let start = self.capture_stack.pop().unwrap();
        &self.input[start..self.pos]
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
        if remaining.len() == 0 { return Err(()); }
        let start = self.pos;
        for c in remaining.chars() {
            if chars.find(c) != None { break; }
            self.ch += 1;
            self.pos += 1;
        }
        if self.pos != start {
            Ok(&self.input[start..self.pos])
        } else {
            Err(())
        }
    }

    pub fn consume_chars(&mut self, chars:&str) -> Result<&'a str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        if remaining.len() == 0 { return Err(()); }
        let start = self.pos;
        for c in remaining.chars() {
            if chars.find(c) == None { break; }
            self.ch += 1;
            self.pos += 1;
        }
        if self.pos != start {
            Ok(&self.input[start..self.pos])
        } else {
            Err(())
        }
    }

    pub fn consume_while(&mut self, pred:fn(char) -> bool) -> Result<&'a str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        if remaining.len() == 0 { return Err(()); }
        let start = self.pos;
        for c in remaining.chars() {
            if !pred(c) { break; }
            if c == '\n' {
                self.ch = 0;
                self.line += 1;
            } else {
                self.ch += 1;
            }
            self.pos += 1;
        }
        if self.pos != start {
            Ok(&self.input[start..self.pos])
        } else {
            Err(())
        }
    }

    pub fn consume<'b>(&mut self, token:&'b str) -> Result<&'b str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        if remaining.len() == 0 { return Err(()); }
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

    pub fn consume_until<T>(&mut self, pred:fn(&mut ParseState<'a>) -> ParseResult<'a, T>) -> (Result<&'a str, ()>, ParseResult<'a, T>) {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        let start = self.pos;
        for c in remaining.chars() {
            let pre_check_pos = self.pos;
            let result = pred(self);
            match result {
                ParseResult::Ok(..) => return (Ok(&self.input[start..pre_check_pos]), result),
                ParseResult::Error(..) => return (Ok(&self.input[start..pre_check_pos]), result),
                _ => {}
            }
            if c == '\n' {
                self.ch = 0;
                self.line += 1;
            } else {
                self.ch += 1;
            }
            self.pos += 1;
        }
        (Ok(&self.input[start..self.pos]), ParseResult::Fail(MatchType::ConsumeUntil))
    }

    pub fn fail<'b, T>(&mut self, with:MatchType<'b>) -> ParseResult<'b, T> {
        self.backtrack();
        ParseResult::Fail(with)
    }

    pub fn error<'b, T>(&mut self, with:ParseError) -> ParseResult<'b, T> {
        let err = self.make_error(with);
        self.pop();
        err
    }

    pub fn make_error<'b, T>(&self, with:ParseError) -> ParseResult<'b, T> {
        ParseResult::Error(self.freeze(), with)
    }

    pub fn freeze(&self) -> FrozenParseState {
        let frozen_stack = self.stack.iter().map(|&(a,b,c,d,e)| (a.to_string(), b, c, d, e)).collect();
        FrozenParseState { pos:self.pos, ignore_space:self.ignore_space, stack: frozen_stack, line:self.line, ch:self.ch }
    }
}

//--------------------------------------------------------------------
// Combinator predicates
//--------------------------------------------------------------------

#[inline]
pub fn is_alphabetic(chr:char) -> bool {
    chr.is_alphabetic()
}

#[inline]
pub fn is_digit(chr:char) -> bool {
    chr.is_numeric()
}

#[inline]
pub fn is_alphanumeric(chr:char) -> bool {
    chr.is_alphanumeric()
}
