
use nom::{digit};
use std::str::{self, FromStr};

#[derive(Debug)]
pub enum Node<'a> {
    Integer(i32),
    Float(f32),
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

named!(expr<Node<'a>>,
       ws!(alt_complete!(
               number |
               identifier => { |v:&'a str| Node::Variable(v) }
                        )));

named!(equality<Node<'a>>,
       ws!(alt_complete!(

                        ))));

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
           // attrs: hashtag >>
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
    println!("{:?}", hashtag(b"#fo|o"));
    println!("{:?}", block(b"search [#foo woah:zomg x > 3.2] bind [#bar]"));
}
