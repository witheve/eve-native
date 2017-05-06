
use nom::{alphanumeric};
use std::str::{self};

#[derive(Debug)]
pub enum Node<'a> {
    Tag(&'a str),
    Record(Vec<Node<'a>>),
}

named!(identifier<&str>, map_res!(is_not_s!("@#\\.,()[]{}:\"|;"), str::from_utf8));

named!(hashtag<Node>,
       do_parse!(
           tag!("#") >>
           tag_name: identifier >>
           (Node::Tag(tag_name))
                )
       );

named!(record<Node<'a>>,
       do_parse!(
           // attrs: hashtag >>
           tag!("[") >>
           attrs: many0!(hashtag) >>
           tag!("]") >>
           (Node::Record(attrs))
                )
       );

#[test]
fn parser_coolness() {
    println!("{:?}", hashtag(b"#fo|o"));
    println!("{:?}", record(b"[#zomg#woop]"));
}
