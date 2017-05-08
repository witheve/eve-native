
use nom::{digit, alphanumeric, anychar};
use std::str::{self, FromStr};

#[derive(Debug)]
pub enum Node<'a> {
    Integer(i32),
    Float(f32),
    RawString(&'a str),
    EmbeddedString(Vec<Node<'a>>),
    Tag(&'a str),
    Variable(&'a str),
    Attribute(&'a str),
    AttributeEquality(&'a str, Box<Node<'a>>),
    AttributeInequality {attribute:&'a str, right:Box<Node<'a>>, op:&'a str},
    Record(Vec<Node<'a>>),
    Search(Vec<Node<'a>>),
    Bind(Vec<Node<'a>>),
    Commit(Vec<Node<'a>>),
    Block{search:Box<Option<Node<'a>>>, update:Box<Node<'a>>},
}

named!(identifier<&str>, map_res!(is_not_s!("#\\.,()[]{}:\"|; \r\n\t"), str::from_utf8));

named!(number<Node<'a>>,
       alt_complete!(
           recognize!(delimited!(digit, tag!("."), digit)) => { |v:&[u8]| {
               let s = str::from_utf8(v).unwrap();
               Node::Float(f32::from_str(s).unwrap())
           }} |
           recognize!(digit) => {|v:&[u8]| {
               let s = str::from_utf8(v).unwrap();
               Node::Integer(i32::from_str(s).unwrap())
           }}));

named!(raw_string<&str>,
       delimited!(
           tag!("\""),
           map_res!(escaped!(is_not_s!("\"\\"), '\\', one_of!("\"\\")), str::from_utf8),
           tag!("\"")
       ));

named!(string_embed<Node<'a>>,
       delimited!(
           tag!("{{"),
           expr,
           tag!("}}")
       ));

// @FIXME: seems like there should be a better way to handle this
named!(not_embed_start<&[u8]>, is_not_s!("{"));
named!(string_parts<Vec<Node<'a>>>,
       fold_many1!(
           alt_complete!(
               string_embed |
               map_res!(not_embed_start, str::from_utf8) => { |v:&'a str| Node::RawString(v) } |
               map_res!(recognize!(pair!(tag!("{"), not_embed_start)), str::from_utf8) => { |v:&'a str| Node::RawString(v) }),
           Vec::new(),
           |mut acc: Vec<Node<'a>>, cur: Node<'a>| {
               acc.push(cur);
               acc
           }));

named!(string<Node<'a>>,
       do_parse!(
           raw: raw_string >>
           ({
               let mut info = string_parts(raw.as_bytes());
               let mut parts = info.unwrap().1;
               if parts.len() == 1 {
                   parts.pop().unwrap()
               } else {
                   Node::EmbeddedString(parts)
               }
           })));

named!(expr<Node<'a>>,
       ws!(alt_complete!(
               number |
               string |
               identifier => { |v:&'a str| Node::Variable(v) }
                        )));

// named!(equality<Node<'a>>,
//        ws!(alt_complete!(

//                         )));

named!(hashtag<Node>,
       do_parse!(
           tag!("#") >>
           tag_name: identifier >>
           (Node::Tag(tag_name))));

named!(attribute_inequality<Node<'a>>,
       do_parse!(
           attribute: identifier >>
           op: ws!(alt!(tag!(">") | tag!(">=") | tag!("<") | tag!("<=") | tag!("!="))) >>
           right: expr >>
           (Node::AttributeInequality{attribute, right:Box::new(right), op:str::from_utf8(op).unwrap()})));


named!(attribute_equality<Node<'a>>,
       do_parse!(
           attr: identifier >>
           ws!(alt!(tag!(":") | tag!("="))) >>
           value: expr >>
           (Node::AttributeEquality(attr, Box::new(value)))));

named!(attribute<Node<'a>>,
       ws!(alt_complete!(
               hashtag |
               attribute_equality |
               attribute_inequality |
               identifier => { |v:&'a str| Node::Attribute(v) })));

named!(record<Node<'a>>,
       do_parse!(
           tag!("[") >>
           attrs: many0!(attribute) >>
           tag!("]") >>
           (Node::Record(attrs))));

named!(search_section<Node<'a>>,
       do_parse!(
           ws!(tag!("search")) >>
           items: many0!(ws!(record)) >>
           (Node::Search(items))));

named!(bind_section<Node<'a>>,
       do_parse!(
           ws!(tag!("bind")) >>
           items: many0!(ws!(record)) >>
           (Node::Bind(items))));

named!(commit_section<Node<'a>>,
       do_parse!(
           ws!(tag!("commit")) >>
           items: many0!(ws!(record)) >>
           (Node::Commit(items))));

named!(block<Node<'a>>,
       ws!(do_parse!(
               search: opt!(search_section) >>
               update: alt_complete!(
                   bind_section |
                   commit_section
                   ) >>
               (Node::Block {search:Box::new(search), update:Box::new(update)}))));

#[test]
fn parser_coolness() {
    println!("{:?}", string_parts(b"\"h {} ey\""));
    println!("{:?}", block(b"search [#foo woah:zomg x > 3.2] bind [#bar]"));
    println!("{:?}", block(b"bind [#bar x:\"dude this {{yo}} is cool\"]"));
}
