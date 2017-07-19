
use parser::{Node, OutputType};
use std::str::FromStr;

//--------------------------------------------------------------------
// Combinator Macros
//--------------------------------------------------------------------

macro_rules! call (
    ($state:ident, $func:ident) => ({
        let result = $func($state);
        match result {
            ParseResult::Ok(value) => { value }
            _ => return result,
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
macro_rules! tag (($name:ident, $tag:expr) => (
    let v = $tag;
    if let Err(_) = $name.consume(v) {
        return $name.fail(MatchType::Tag(v));
    }
));

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
                Err(_) => {
                    return $name.fail(MatchType::AnyExcept(v));
                }
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
    ($name:ident, $chars:ident | $err:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => {
                    if cur.len() == 0 {
                        return $name.error(ParseError::$err);
                    }
                    cur
                },
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
                Ok(cur) => {
                    if cur.len() == 0 {
                        return $name.fail(MatchType::TakeWhile);
                    }
                    cur
                },
                Err(_) => {
                    return $name.fail(MatchType::TakeWhile);
                }
            }
        });
    ($name:ident, $chars:ident | $err:ident) => ({
            let v = $chars;
            match $name.consume_while(v) {
                Ok(cur) => {
                    if cur.len() == 0 {
                        return $name.error(ParseError::$err);
                    }
                    cur
                },
                Err(_) => {
                    return $name.error(ParseError::$err);
                }
            }
        });
);


macro_rules! many_n (
    ($state:ident, $n:expr, $err:ident, $func:ident) => ({
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
        });
    ($state:ident, $n:expr, $func:ident) => ({
            let mut items = vec![];
            loop {
                let result = $func($state);
                match result {
                    ParseResult::Error(..) => { return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            if items.len() <= $n {
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
                    ParseResult::Error(..) => { return result; }
                    ParseResult::Ok(value) => { items.push(value); }
                    ParseResult::Fail(..) => { break }
                }
            }
            items
        });
);
#[macro_export] macro_rules! many (($state:ident, $func:ident) => ( many_n!($state, $func); ); );
#[macro_export] macro_rules! many_1 (
    ($state:ident, $func:ident | $err:ident) => ( many_n!($state, 1, $err, $func); );
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
    ($state:ident, [ $($rest:ident)* ] | $err:ident) => (
        {
            let result = alt_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return $state.error(ParseError::$err); }
                ParseResult::Error(..) => { return result; }
                ParseResult::Ok(v) => v
            }
        });
    ($state:ident, [ $($rest:ident)* ]) => (
        {
            let result = alt_branch!($state $(, $rest)*);
            match result {
                ParseResult::Fail(..) => { return ParseResult::Fail(MatchType::Alternative) }
                ParseResult::Error(..) => { return result; }
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
    ($state:ident, [ $($rest:expr)* ] | $err:ident) => (
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
                ParseResult::Fail(..) => { return ParseResult::Fail(MatchType::Alternative); }
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
    Alternative,
    TakeWhile,
    Tag(&'a str),
    AnyExcept(&'a str),
    Many(usize),
}

//--------------------------------------------------------------------
// Parse State
//--------------------------------------------------------------------

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
    capture_stack: Vec<usize>,
    line: usize,
    ch: usize,
    pos: usize,
    ignore_space: bool,
}

impl<'a> ParseState<'a> {
    pub fn new(input:&str) -> ParseState {
        ParseState { input, stack:vec![], capture_stack:vec![], line:0, ch:0, pos:0, ignore_space: false }
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
        let start = self.pos;
        for c in remaining.chars() {
            if chars.find(c) != None { break; }
            self.ch += 1;
            self.pos += 1;
        }
        Ok(&self.input[start..self.pos])
    }

    pub fn consume_chars(&mut self, chars:&str) -> Result<&'a str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
        let start = self.pos;
        for c in remaining.chars() {
            if chars.find(c) == None { break; }
            self.ch += 1;
            self.pos += 1;
        }
        Ok(&self.input[start..self.pos])
    }

    pub fn consume_while(&mut self, pred:fn(char) -> bool) -> Result<&'a str, ()> {
        if self.ignore_space { self.eat_space(); }
        let remaining = &self.input[self.pos..];
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

//--------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------

const BREAK_CHARS:&'static str = "#\\.,()[]{}:=\"|; \r\n\t";

//--------------------------------------------------------------------
// Identifiers and variables
//--------------------------------------------------------------------

parser!(identifier(state) -> Node<'a> {
    let v = any_except!(state, BREAK_CHARS);
    result!(state, Node::Identifier(v))
});

parser!(variable(state) -> Node<'a> {
    let v = any_except!(state, BREAK_CHARS);
    result!(state, Node::Variable(v))
});

//--------------------------------------------------------------------
// Numbers
//--------------------------------------------------------------------

whitespace_parser!(float(state) -> Node<'a> {
    state.capture();
    // -? [0-9]+ \. [0-9]+
    any!(state, "-"); take_while_1!(state, is_digit); tag!(state, "."); take_while_1!(state, is_digit);
    let number = f32::from_str(state.stop_capture()).unwrap();
    result!(state, Node::Float(number))
});

whitespace_parser!(integer(state) -> Node<'a> {
    state.capture();
    // -? [0-9]+
    any!(state, "-"); take_while_1!(state, is_digit);
    let number = i32::from_str(state.stop_capture()).unwrap();
    result!(state, Node::Integer(number))
});

parser!(number(state) -> Node<'a> {
    let num = alt!(state, [float integer]);
    result!(state, num)
});

//--------------------------------------------------------------------
// Strings
//--------------------------------------------------------------------

parser!(escaped_quote(state) -> Node<'a> {
    tag!(state, "\\\"");
    result!(state, Node::RawString("\""))
});

parser!(string_embed(state) -> Node<'a> {
    tag!(state, "{{");
    let embed = call!(state, expression);
    tag!(state, "}}");
    result!(state, embed)
});

parser!(string_bracket(state) -> Node<'a> {
    tag!(state, "{");
    result!(state, Node::RawString("{"))
});

parser!(string_chars(state) -> Node<'a> {
    let chars = any_except!(state, "\"{");
    result!(state, Node::RawString(chars))
});

parser!(string_parts(state) -> Node<'a> {
    let part = alt!(state, [ escaped_quote string_embed string_bracket string_chars ]);
    result!(state, part)
});

parser!(string(state) -> Node<'a> {
    tag!(state, "\"");
    let mut parts = many!(state, string_parts);
    tag!(state, "\"");
    let result = match (parts.len(), parts.get(0)) {
        (1, Some(&Node::RawString(_))) => parts.pop().unwrap(),
        _ => Node::EmbeddedString(None, parts)
    };
    result!(state, result)
});

//--------------------------------------------------------------------
// values and expressions
//--------------------------------------------------------------------

parser!(value(state) -> Node<'a> {
    let part = alt!(state, [ number string /* record_function record_reference */ wrapped_expression ]);
    result!(state, part)
});

parser!(wrapped_expression(state) -> Node<'a> {
    tag!(state, "(");
    let value = call!(state, expression);
    tag!(state, ")");
    result!(state, value)
});

parser!(expression(state) -> Node<'a> {
    let part = alt!(state, [ /* infix_addition infix_multiplication */ value ]);
    result!(state, part)
});

parser!(expression_set(state) -> Node<'a> {
    tag!(state, "(");
    let exprs = many_1!(state, expression | EmptyUpdate);
    tag!(state, ")");
    result!(state, Node::ExprSet(exprs))
});

//--------------------------------------------------------------------
// Infix
//--------------------------------------------------------------------

parser!(infix_addition(state) -> Node<'a> {
    let left = alt!(state, [ infix_multiplication value ]);
    let op = alt_tag!(state, [ "+" "-" ]);
    let right = call!(state, expression);
    result!(state, Node::Infix { result:None, left:Box::new(left), right:Box::new(right), op })
});

parser!(infix_multiplication(state) -> Node<'a> {
    let left = call!(state, value);
    let op = alt_tag!(state, [ "*" "/" ]);
    let right = alt!(state, [ infix_multiplication value ]);
    result!(state, Node::Infix { result:None, left:Box::new(left), right:Box::new(right), op })
});

parser!(equality(state) -> Node<'a> {
    let left = call!(state, expression);
    tag!(state, "=");
    let right = alt!(state, [ expression record ]);
    result!(state, Node::Equality { left:Box::new(left), right:Box::new(right) })
});

parser!(inequality(state) -> Node<'a> {
    let left = call!(state, expression);
    let op = alt_tag!(state, [ ">=" "<=" "!=" "<" ">" ]);
    let right = call!(state, expression);
    result!(state, Node::Inequality { left:Box::new(left), right:Box::new(right), op })
});

//--------------------------------------------------------------------
// Tags, Attributes
//--------------------------------------------------------------------

parser!(hashtag(state) -> Node<'a> {
    tag!(state, "#");
    let name = match call!(state, identifier) {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    result!(state, Node::Tag(name))
});

parser!(attribute_variable(state) -> Node<'a> {
    let attr = match call!(state, identifier) {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    result!(state, Node::Attribute(attr))
});

parser!(attribute_equality(state) -> Node<'a> {
    let attr = match call!(state, identifier) {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    alt_tag!(state, [ ":" "=" ]);
    let value = alt!(state, [ record_set expression expression_set ]);
    result!(state, Node::AttributeEquality(attr, Box::new(value)))
});

parser!(attribute_inequality(state) -> Node<'a> {
    let attribute = match call!(state, identifier) {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    let op = alt_tag!(state, [ ">=" "<=" "!=" "<" ">" ]);
    let right = call!(state, expression);
    result!(state, Node::AttributeInequality { attribute, right:Box::new(right), op })
});

parser!(attribute(state) -> Node<'a> {
    let part = alt!(state, [ hashtag attribute_equality attribute_inequality attribute_variable ]);
    result!(state, part)
});

//--------------------------------------------------------------------
// Records
//--------------------------------------------------------------------

parser!(record(state) -> Node<'a> {
    tag!(state, "[");
    let attributes = many!(state, attribute);
    tag!(state, "]");
    result!(state, Node::Record(None, attributes))
});

parser!(record_set(state) -> Node<'a> {
    let records = many_1!(state, record);
    result!(state, Node::RecordSet(records))
});

parser!(wrapped_record_set(state) -> Node<'a> {
    tag!(state, "(");
    let set = call!(state, record_set);
    tag!(state, ")");
    result!(state, set)
});

//--------------------------------------------------------------------
// Functions and lookup
//--------------------------------------------------------------------

parser!(function_attribute(state) -> Node<'a> {
    let part = alt!(state, [ attribute_equality attribute_variable ]);
    result!(state, part)
});

parser!(lookup(state, output_type:OutputType) -> Node<'a> {
    tag!(state, "lookup[");
    let attributes = many!(state, function_attribute);
    tag!(state, "]");
    result!(state, Node::RecordLookup(attributes, output_type))
});

parser!(record_function(state) -> Node<'a> {
    let op = match call!(state, identifier) {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    tag!(state, "[");
    let params = many!(state, function_attribute);
    tag!(state, "]");
    result!(state, Node::RecordFunction { op, params, outputs:vec![] })
});

parser!(multi_equality_left(state) -> Node<'a> {
    let part = alt!(state, [ expression_set variable ]);
    result!(state, part)
});

parser!(multi_function_equality(state) -> Node<'a> {
    let neue_outputs = match call!(state, expression_set) {
        Node::ExprSet(items) => items,
        me @ Node::Variable(_) => vec![me],
        _ => unreachable!()
    };
    tag!(state, "=");
    let mut func = call!(state, record_function);
    match func {
        Node::RecordFunction { ref mut outputs, .. } => {
           *outputs = neue_outputs;
        }
        _ => unreachable!()
    };
    result!(state, func)
});

//--------------------------------------------------------------------
// Watch and Project
//--------------------------------------------------------------------

parser!(project_section(state) -> Node<'a> {
    tag!(state, "project");
    tag!(state, "(");
    let items = many_1!(state, expression | EmptyUpdate);
    tag!(state, ")");
    result!(state, Node::Project(items))
});
