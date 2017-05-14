
use nom::{digit, alphanumeric, anychar, IResult};
use std::str::{self, FromStr};
use std::collections::HashMap;
use ops::{Interner, Field, Constraint, register, Program, make_scan, make_filter, make_function, Transaction, Block};

#[derive(Debug, Clone)]
pub enum Node<'a> {
    Integer(i32),
    Float(f32),
    RawString(&'a str),
    EmbeddedString(Option<String>, Vec<Node<'a>>),
    Tag(&'a str),
    Variable(&'a str),
    Attribute(&'a str),
    AttributeEquality(&'a str, Box<Node<'a>>),
    AttributeInequality {attribute:&'a str, right:Box<Node<'a>>, op:&'a str},
    Inequality {left:Box<Node<'a>>, right:Box<Node<'a>>, op:&'a str},
    Equality {left:Box<Node<'a>>, right:Box<Node<'a>>},
    Infix {result:Option<String>, left:Box<Node<'a>>, right:Box<Node<'a>>, op:&'a str},
    Record(Option<String>, Vec<Node<'a>>),
    OutputRecord(Option<String>, Vec<Node<'a>>),
    Search(Vec<Node<'a>>),
    Bind(Vec<Node<'a>>),
    Commit(Vec<Node<'a>>),
    Block{search:Box<Option<Node<'a>>>, update:Box<Node<'a>>},
}

// match self {
//     &Node::Integer(v) => {}
//     &Node::Float(v) => {},
//     &Node::RawString(v) => {},
//     &Node::EmbeddedString(ref vs) => {},
//     &Node::Tag(v) => {},
//     &Node::Variable(v) => {},
//     &Node::Attribute(v) => {},
//     &Node::AttributeEquality(a, ref v) => {},
//     &Node::AttributeInequality {ref attribute, ref right, ref op} => {},
//     &Node::Record(ref attrs) => {},
//     &Node::Search(ref statements) => {},
//     &Node::Bind(ref statements) => {},
//     &Node::Commit(ref statements) => {},
//     &Node::Block{ref search, ref update} => {},
// }
//
//let constraints = vec![
//   make_scan(register(0), program.interner.string("tag"), program.interner.string("person")),
//   make_scan(register(0), program.interner.string("age"), register(1)),
//   make_filter(">", register(1), program.interner.number(60.0)),
//   make_function("+", vec![register(1), program.interner.number(10.0)], register(2)),
//   Constraint::Insert {e: register(0), a: program.interner.string("adjsted-age"), v: register(2)},
// ];
// program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });

impl<'a> Node<'a> {

    pub fn unify(&mut self, comp:&mut Compilation) {
        self.gather_equalities(comp);
        let mut values:HashMap<Field, Field> = HashMap::new();
        for v in comp.vars.values() {
            values.insert(Field::Register(*v), Field::Register(*v));
        }
        let mut changed = true;
        // go in rounds and try to unify everything
        while changed {
            changed = false;
            for &(l, r) in comp.equalities.iter() {
                let left_value:Field = if let Field::Register(_) = l { values.entry(l).or_insert(l).clone() } else { l };
                let right_value:Field = if let Field::Register(_) = l { values.entry(r).or_insert(r).clone() } else { r };
                match (left_value, right_value) {
                    (Field::Register(l_reg), Field::Register(r_reg)) => {
                        if l_reg < r_reg {
                            values.insert(r, left_value.clone());
                            changed = true;
                        } else if r_reg < r_reg {
                            values.insert(l, right_value.clone());
                            changed = true;
                        }
                    },
                    (Field::Register(_), other) => {
                        values.insert(l, other.clone());
                        changed = true;
                    },
                    (other, Field::Register(_)) => {
                        values.insert(r, other.clone());
                        changed = true;
                    },
                    (a, b) => { if a != b { panic!("Invalid equality {:?} != {:?}", a, b); } },
                }
            }
        }
        let mut final_map = HashMap::new();
        let mut real_registers = HashMap::new();
        for value in values.values() {
            if let &Field::Register(_) = value {
                let size = real_registers.len();
                real_registers.entry(value).or_insert_with(|| Field::Register(size));
            }
        }
        for (k, v) in values.iter() {
            let neue = if let Some(neue_reg) = real_registers.get(&v) {
                neue_reg
            } else {
                v
            };
            final_map.insert(k.clone(), neue.clone());
        }
        println!("Unified: {:?}", final_map);
        comp.reg_count = real_registers.len();
        comp.var_values = final_map;
    }

    pub fn gather_equalities(&mut self, comp:&mut Compilation) -> Option<Field> {
        match self {
            &mut Node::Tag(_) => { None },
            &mut Node::Integer(v) => { Some(comp.interner.number(v as f32)) }
            &mut Node::Float(v) => { Some(comp.interner.number(v)) },
            &mut Node::RawString(v) => { Some(comp.interner.string(v)) },
            &mut Node::Variable(v) => { Some(comp.get_register(v)) },
            &mut Node::Attribute(_) => { None },
            &mut Node::AttributeEquality(a, ref mut v) => { v.gather_equalities(comp) },
            &mut Node::Inequality {ref left, ref right, ref op} => {
                None
            },
            &mut Node::EmbeddedString(ref mut var, ref vs) => {
                println!("HERE!!");
                let var_name = format!("__eve_concat{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&var_name);
                *var = Some(var_name);
                Some(reg)

            },
            &mut Node::Equality {ref mut left, ref mut right} => {
                let l = left.gather_equalities(comp).unwrap();
                let r = right.gather_equalities(comp).unwrap();
                comp.equalities.push((l,r));
                None
            },
            &mut Node::Infix {ref mut result, ref left, ref right, ..} => {
                let result_name = format!("__eve_infix{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&result_name);
                *result = Some(result_name);
                Some(reg)
            },
            &mut Node::Record(ref mut var, ref mut attrs) => {
                for attr in attrs {
                    attr.gather_equalities(comp);
                }
                let var_name = format!("__eve_record{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&var_name);
                *var = Some(var_name);
                Some(reg)
            },
            &mut Node::OutputRecord(ref mut var, ref mut attrs) => {
                for attr in attrs {
                    attr.gather_equalities(comp);
                }
                let var_name = format!("__eve_output_record{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&var_name);
                *var = Some(var_name);
                Some(reg)
            },
            &mut Node::Search(ref mut statements) => {
                for s in statements {
                    s.gather_equalities(comp);
                };
                None
            },
            &mut Node::Bind(ref mut statements) => {
                for s in statements {
                    s.gather_equalities(comp);
                };
                None
            },
            &mut Node::Commit(ref mut statements) => {
                for s in statements {
                    s.gather_equalities(comp);
                };
                None
            },
            &mut Node::Block{ref mut search, ref mut update} => {
                if let Some(ref mut s) = **search {
                    s.gather_equalities(comp);
                };
                update.gather_equalities(comp);
                None
            },
            _ => panic!("Trying to gather equalities on {:?}", self)
        }
    }

    pub fn compile(&self, comp:&mut Compilation) -> Option<Field> {
        match self {
            &Node::Integer(v) => { Some(comp.interner.number(v as f32)) }
            &Node::Float(v) => { Some(comp.interner.number(v)) },
            &Node::RawString(v) => { Some(comp.interner.string(v)) },
            &Node::Variable(v) => { Some(comp.get_value(v)) },
            &Node::AttributeEquality(a, ref v) => { v.compile(comp) },
            &Node::Inequality {ref left, ref right, ref op} => {
                let left_value = left.compile(comp);
                let right_value = right.compile(comp);
                match (left_value, right_value) {
                    (Some(l), Some(r)) => {
                        comp.constraints.push(make_filter(op, l, r));
                    },
                    _ => panic!("inequality without both a left and right: {:?} {} {:?}", left, op, right)
                }
                right_value
            },
            &Node::EmbeddedString(ref var, ref vs) => {
                let resolved = vs.iter().map(|v| v.compile(comp).unwrap()).collect();
                if let &Some(ref name) = var {
                    let mut out_reg = comp.get_register(name);
                    let out_value = comp.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        comp.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    comp.constraints.push(make_function("concat", resolved, out_reg));
                    Some(out_reg)
                } else {
                    panic!("Embedded string without a result assigned {:?}", self);
                }

            },
            &Node::Infix { ref op, ref result, ref left, ref right } => {
                let left_value = left.compile(comp).unwrap();
                let right_value = right.compile(comp).unwrap();
                if let &Some(ref name) = result {
                    let mut out_reg = comp.get_register(name);
                    let out_value = comp.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        comp.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    comp.constraints.push(make_function(op, vec![left_value, right_value], out_reg));
                    Some(out_reg)
                } else {
                    panic!("Infix without a result assigned {:?}", self);
                }
            },
            &Node::Equality {ref left, ref right} => {
                left.compile(comp);
                right.compile(comp);
                None
            },
            &Node::Record(ref var, ref attrs) => {
                let reg = if let &Some(ref name) = var {
                    comp.get_value(name)
                } else {
                    panic!("Record missing a var {:?}", var)
                };
                for attr in attrs {
                    let (a, v) = match attr {
                        &Node::Tag(t) => { (comp.interner.string("tag"), comp.interner.string(t)) },
                        &Node::Attribute(a) => { (comp.interner.string(a), comp.get_value(a)) },
                        &Node::AttributeEquality(a, ref v) => { (comp.interner.string(a), v.compile(comp).unwrap()) },
                        &Node::AttributeInequality {ref attribute, ref op, ref right } => {
                            let reg = comp.get_register(attribute);
                            let right_value = right.compile(comp);
                            match right_value {
                                Some(r) => {
                                    comp.constraints.push(make_filter(op, reg, r));
                                },
                                _ => panic!("inequality without both a left and right: {} {} {:?}", attribute, op, right)
                            }
                            (comp.interner.string(attribute), reg)
                        },
                        _ => { panic!("TODO") }
                    };
                    comp.constraints.push(make_scan(reg, a, v));
                };
                Some(reg)
            },
            &Node::OutputRecord(ref var, ref attrs) => {
                let reg = if let &Some(ref name) = var {
                    comp.get_value(name)
                } else {
                    panic!("Record missing a var {:?}", var)
                };
                let mut identity_attrs = vec![];
                for attr in attrs {
                    let (a, v) = match attr {
                        &Node::Tag(t) => { (comp.interner.string("tag"), comp.interner.string(t)) },
                        &Node::Attribute(a) => { (comp.interner.string(a), comp.get_value(a)) },
                        &Node::AttributeEquality(a, ref v) => { (comp.interner.string(a), v.compile(comp).unwrap()) },
                        _ => { panic!("TODO") }
                    };
                    identity_attrs.push(v);
                    comp.constraints.push(Constraint::Insert{e:reg, a, v});
                };
                comp.constraints.push(make_function("gen_id", identity_attrs, reg));
                Some(reg)
            },
            &Node::Search(ref statements) => {
                for s in statements {
                    s.compile(comp);
                };
                None
            },
            &Node::Bind(ref statements) => {
                for s in statements {
                    s.compile(comp);
                };
                None
            },
            &Node::Commit(ref statements) => {
                for s in statements {
                    s.compile(comp);
                };
                None
            },
            &Node::Block{ref search, ref update} => {
                if let Some(ref s) = **search {
                    s.compile(comp);
                };
                update.compile(comp);
                None
            },
            _ => panic!("Trying to compile something we don't know how to compile {:?}", self)
        }
    }
}

pub struct Compilation<'a> {
    vars: HashMap<String, usize>,
    var_values: HashMap<Field, Field>,
    interner: &'a mut Interner,
    constraints: Vec<Constraint>,
    equalities: Vec<(Field, Field)>,
    id: usize,
    reg_count: usize,
}

impl<'a> Compilation<'a> {
    pub fn new(interner: &'a mut Interner) -> Compilation<'a> {
        Compilation { vars:HashMap::new(), var_values:HashMap::new(), interner, constraints:vec![], equalities:vec![], id:0, reg_count:0 }
    }

    pub fn get_register(&mut self, name: &str) -> Field {
        let len = self.vars.len();
        let ix = *self.vars.entry(name.to_string()).or_insert(len);
        register(ix)
    }

    pub fn get_value(&mut self, name: &str) -> Field {
        let reg = self.get_register(name);
        let reg_count = self.reg_count;
        let mut needs_update = false;
        let val = self.var_values.entry(reg).or_insert_with(|| {
            needs_update = true;
            Field::Register(reg_count)
        }).clone();
        if needs_update {
            self.reg_count += 1;
        }
        val
    }
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
                   Node::EmbeddedString(None, parts)
               }
           })));

named!(value<Node<'a>>,
       ws!(alt_complete!(
               number |
               string |
               identifier => { |v:&'a str| Node::Variable(v) }
               )));

named!(expr<Node<'a>>,
       ws!(alt_complete!(
               infix_addition |
               infix_multiplication |
               value
               )));

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
           (Node::Record(None, attrs))));

named!(inequality<Node<'a>>,
       do_parse!(
           left: expr >>
           op: ws!(alt!(tag!(">") | tag!(">=") | tag!("<") | tag!("<=") | tag!("!="))) >>
           right: expr >>
           (Node::Inequality{left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(infix_addition<Node<'a>>,
       do_parse!(
           left: alt_complete!(infix_multiplication | value) >>
           op: ws!(alt!(tag!("+") | tag!("-"))) >>
           right: expr >>
           (Node::Infix{result:None, left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(infix_multiplication<Node<'a>>,
       do_parse!(
           left: value >>
           op: ws!(alt!(tag!("*") | tag!("/"))) >>
           right: alt_complete!(infix_multiplication | value) >>
           (Node::Infix{result:None, left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(equality<Node<'a>>,
       do_parse!(
           left: expr >>
           op: ws!(tag!("=")) >>
           right: expr >>
           (Node::Equality {left:Box::new(left), right:Box::new(right)})));

named!(output_attribute<Node<'a>>,
       ws!(alt_complete!(
               hashtag |
               attribute_equality |
               identifier => { |v:&'a str| Node::Attribute(v) })));

named!(output_record<Node<'a>>,
       do_parse!(
           tag!("[") >>
           attrs: many0!(output_attribute) >>
           tag!("]") >>
           (Node::OutputRecord(None, attrs))));

named!(search_section<Node<'a>>,
       do_parse!(
           ws!(tag!("search")) >>
           items: many0!(ws!(alt_complete!(
                            record |
                            inequality |
                            equality
                        ))) >>
           (Node::Search(items))));

named!(bind_section<Node<'a>>,
       do_parse!(
           ws!(tag!("bind")) >>
           items: many0!(ws!(output_record)) >>
           (Node::Bind(items))));

named!(commit_section<Node<'a>>,
       do_parse!(
           ws!(tag!("commit")) >>
           items: many0!(ws!(output_record)) >>
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
    // println!("{:?}", expr(b"3 * 4 + 5 * 6"));
    // println!("{:?}", expr(b"3 * 4 * 5 * 6"));
    let b = block(b"search [#foo woah] a = woah + 10 bind [#bar text:\"{{woah}} - {{a}}\"]");
    println!("{:?}", b);
    let mut program = Program::new();
    let block = {
        let mut comp = Compilation::new(&mut program.interner);
        match b {
            IResult::Done(_, mut block) => {
                block.unify(&mut comp);
                block.compile(&mut comp);
            }
            _ => { println!("Failed: {:?}", b); }
        }

        for c in comp.constraints.iter() {
            println!("{:?}", c);
        }

        Block { name: "simple block".to_string(), constraints:comp.constraints, pipes: vec![] }
    };
    program.register_block(block);
    let mut txn = Transaction::new();
    txn.input(program.interner.number_id(1.0), program.interner.string_id("tag"), program.interner.string_id("foo"), 1);
    txn.input(program.interner.number_id(1.0), program.interner.string_id("woah"), program.interner.number_id(1000.0), 1);
    txn.exec(&mut program);
}
