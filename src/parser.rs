use nom::{digit, alphanumeric, anychar, IResult, Err};
use std::str::{self, FromStr};
use std::collections::{HashMap, HashSet};
use ops::{Interner, Field, Constraint, register, Program, make_scan, make_filter, make_function, Transaction, Block};
use std::error::Error;
use std::io::prelude::*;
use std::fs::File;


lazy_static! {
    static ref FunctionInfo: HashMap<String, HashMap<String, usize>> = {
        let mut m = HashMap::new();
        let mut info = HashMap::new();
        info.insert("degrees".to_string(), 0);
        m.insert("math/sin".to_string(), info);
        let mut info2 = HashMap::new();
        info2.insert("degrees".to_string(), 0);
        m.insert("math/cos".to_string(), info2);
        m
    };
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputType {
    Bind,
    Commit,
}

#[derive(Debug, Clone)]
pub enum Node<'a> {
    Pipe,
    Integer(i32),
    Float(f32),
    RawString(&'a str),
    EmbeddedString(Option<String>, Vec<Node<'a>>),
    Tag(&'a str),
    Variable(&'a str),
    Attribute(&'a str),
    AttributeEquality(&'a str, Box<Node<'a>>),
    AttributeInequality {attribute:&'a str, right:Box<Node<'a>>, op:&'a str},
    AttributeAccess(Vec<&'a str>),
    MutatingAttributeAccess(Vec<&'a str>),
    Inequality {left:Box<Node<'a>>, right:Box<Node<'a>>, op:&'a str},
    Equality {left:Box<Node<'a>>, right:Box<Node<'a>>},
    Infix {result:Option<String>, left:Box<Node<'a>>, right:Box<Node<'a>>, op:&'a str},
    Record(Option<String>, Vec<Node<'a>>),
    RecordSet(Vec<Node<'a>>),
    RecordFunction {result:Option<String>, op:&'a str, params:Vec<Node<'a>>},
    OutputRecord(Option<String>, Vec<Node<'a>>, OutputType),
    RecordUpdate {record:Box<Node<'a>>, value:Box<Node<'a>>, op:&'a str, output_type:OutputType},
    Search(Vec<Node<'a>>),
    Bind(Vec<Node<'a>>),
    Commit(Vec<Node<'a>>),
    Project(Vec<Node<'a>>),
    Watch(&'a str, Vec<Node<'a>>),
    Block{search:Box<Option<Node<'a>>>, update:Box<Node<'a>>},
    Doc { file:String, blocks:Vec<Node<'a>> }
}

impl<'a> Node<'a> {

    pub fn unify(&mut self, comp:&mut Compilation) {
        self.gather_equalities(comp);
        let mut values:HashMap<Field, Field> = HashMap::new();
        let mut provided = HashSet::new();
        for v in comp.vars.values() {
            let field = Field::Register(*v);
            values.insert(field, field);
            if comp.provided_registers.contains(&field) {
                provided.insert(field);
            }
        }
        let mut changed = true;
        // go in rounds and try to unify everything
        while changed {
            changed = false;
            for &(l, r) in comp.equalities.iter() {
                let left_value:Field = if let Field::Register(_) = l { values.entry(l).or_insert(l).clone() } else { l };
                let right_value:Field = if let Field::Register(_) = r { values.entry(r).or_insert(r).clone() } else { r };
                match (left_value, right_value) {
                    (Field::Register(l_reg), Field::Register(r_reg)) => {
                        if l_reg < r_reg {
                            values.insert(r, left_value.clone());
                            if provided.contains(&left_value) {
                                provided.insert(r);
                            }
                            changed = true;
                        } else if r_reg < l_reg {
                            values.insert(l, right_value.clone());
                            if provided.contains(&right_value) {
                                provided.insert(l);
                            }
                            changed = true;
                        }
                    },
                    (Field::Register(_), other) => {
                        values.insert(l, other.clone());
                        provided.insert(l);
                        changed = true;
                    },
                    (other, Field::Register(_)) => {
                        values.insert(r, other.clone());
                        provided.insert(r);
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
        comp.reg_count = real_registers.len();
        comp.var_values = final_map;
        comp.provided_registers = provided;
    }

    pub fn gather_equalities(&mut self, comp:&mut Compilation) -> Option<Field> {
        match self {
            &mut Node::Pipe => { None },
            &mut Node::Tag(_) => { None },
            &mut Node::Integer(v) => { Some(comp.interner.number(v as f32)) }
            &mut Node::Float(v) => { Some(comp.interner.number(v)) },
            &mut Node::RawString(v) => { Some(comp.interner.string(v)) },
            &mut Node::Variable(v) => { Some(comp.get_register(v)) },
            &mut Node::Attribute(_) => { None },
            &mut Node::AttributeInequality {ref mut right, ..} => { right.gather_equalities(comp) },
            &mut Node::AttributeEquality(a, ref mut v) => { v.gather_equalities(comp) },
            &mut Node::Inequality {ref left, ref right, ref op} => {
                None
            },
            &mut Node::EmbeddedString(ref mut var, ref vs) => {
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
            &mut Node::Infix {ref mut result, ref mut left, ref mut right, ..} => {
                left.gather_equalities(comp);
                right.gather_equalities(comp);
                let result_name = format!("__eve_infix{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&result_name);
                *result = Some(result_name);
                Some(reg)
            },
            &mut Node::RecordFunction {ref mut result, ref op, ref mut params} => {
                for param in params {
                    param.gather_equalities(comp);
                }
                let result_name = format!("__eve_infix{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&result_name);
                *result = Some(result_name);
                Some(reg)
            },
            &mut Node::RecordSet(ref mut records) => {
                for record in records {
                    record.gather_equalities(comp);
                }
                None
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
            &mut Node::OutputRecord(ref mut var, ref mut attrs, ..) => {
                for attr in attrs {
                    attr.gather_equalities(comp);
                }
                let var_name = format!("__eve_output_record{}", comp.id);
                comp.id += 1;
                let reg = comp.get_register(&var_name);
                *var = Some(var_name);
                Some(reg)
            },
            &mut Node::AttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                for item in items {
                    final_var.push_str("|");
                    final_var.push_str(item);
                }
                let reg = comp.get_register(&final_var);
                Some(reg)
            },
            &mut Node::MutatingAttributeAccess(ref items) => {
                None
            },
            &mut Node::RecordUpdate {ref mut record, ref op, ref mut value, ..} => {
                let left = record.gather_equalities(comp);
                let right = value.gather_equalities(comp);
                if op == &"<-" {
                    comp.provide(right.unwrap());
                    comp.equalities.push((left.unwrap(), right.unwrap()));
                }
                None
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
            &mut Node::Project(ref mut values) => {
                for v in values {
                    v.gather_equalities(comp);
                };
                None
            },
            &mut Node::Watch(_, ref mut values) => {
                for v in values {
                    v.gather_equalities(comp);
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
            // &Node::AttributeEquality(a, ref v) => { v.compile(comp) },
            &Node::AttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                let mut parent = comp.get_value(items[0]);
                for item in items[1..].iter() {
                    final_var.push_str("|");
                    final_var.push_str(item);
                    let next = comp.get_value(&final_var.to_string());
                    comp.constraints.push(make_scan(parent, comp.interner.string(item), next));
                    parent = next;
                }
                Some(parent)
            },
            &Node::MutatingAttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                let mut parent = comp.get_value(items[0]);
                if items.len() > 2 {
                    for item in items[1..items.len()-2].iter() {
                        final_var.push_str("|");
                        final_var.push_str(item);
                        let next = comp.get_value(&final_var.to_string());
                        comp.constraints.push(make_scan(parent, comp.interner.string(item), next));
                        parent = next;
                    }
                }
                Some(parent)
            },
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
            &Node::RecordFunction { ref op, ref result, ref params} => {
                if let &Some(ref name) = result {
                    let mut out_reg = comp.get_register(name);
                    let out_value = comp.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        comp.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    let info = FunctionInfo.get(*op).unwrap();
                    let mut cur_params = vec![Field::Value(0); info.len() - 1];
                    for param in params {
                        let (a, v) = match param {
                            &Node::Attribute(a) => {
                                (a, comp.get_value(a))
                            }
                            &Node::AttributeEquality(a, ref v) => {
                                (a, v.compile(comp).unwrap())
                            }
                            _ => { panic!("invalid function param: {:?}", param) }
                        };
                        cur_params.insert(info[a], v);
                    }
                    comp.constraints.push(make_function(op, cur_params, out_reg));
                    Some(out_reg)
                } else {
                    panic!("Function without a result assigned {:?}", self);
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
                        &Node::AttributeEquality(a, ref v) => {
                            let result_a = comp.interner.string(a);
                            let result = if let Node::RecordSet(ref records) = **v {
                                for record in records[1..].iter() {
                                    let cur_v = record.compile(comp).unwrap();
                                    comp.constraints.push(make_scan(reg, result_a, cur_v));
                                }
                                records[0].compile(comp).unwrap()
                            } else {
                                v.compile(comp).unwrap()
                            };
                            (result_a, result)
                        },
                        &Node::AttributeInequality {ref attribute, ref op, ref right } => {
                            let reg = comp.get_value(attribute);
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
            &Node::OutputRecord(ref var, ref attrs, ref output_type) => {
                let (reg, needs_id) = if let &Some(ref name) = var {
                    (comp.get_value(name), !comp.is_provided(name))
                } else {
                    panic!("Record missing a var {:?}", var)
                };
                let commit = *output_type == OutputType::Commit;
                let mut identity_contributing = true;
                let mut identity_attrs = vec![];
                for attr in attrs {
                    if let &Node::Pipe = attr {
                        identity_contributing = false;
                        continue;
                    }
                    let (a, v) = match attr {
                        &Node::Tag(t) => { (comp.interner.string("tag"), comp.interner.string(t)) },
                        &Node::Attribute(a) => { (comp.interner.string(a), comp.get_value(a)) },
                        &Node::AttributeEquality(a, ref v) => {
                            let result_a = comp.interner.string(a);
                            let result = if let Node::RecordSet(ref records) = **v {
                                for record in records[1..].iter() {
                                    let cur_v = record.compile(comp).unwrap();
                                    comp.constraints.push(Constraint::Insert{e:reg, a:result_a, v:cur_v, commit});
                                }
                                records[0].compile(comp).unwrap()
                            } else {
                                v.compile(comp).unwrap()
                            };
                            (result_a, result)
                        },
                        _ => { panic!("TODO") }
                    };
                    if identity_contributing {
                        identity_attrs.push(v);
                    }
                    comp.constraints.push(Constraint::Insert{e:reg, a, v, commit});
                };
                if needs_id {
                    comp.constraints.push(make_function("gen_id", identity_attrs, reg));
                }
                Some(reg)
            },
            &Node::RecordUpdate {ref record, ref op, ref value, ref output_type} => {
                // @TODO: compile attribute access correctly
                let (reg, attr) = match **record {
                    Node::MutatingAttributeAccess(ref items) => {
                        let parent = record.compile(comp);
                        (parent.unwrap(), Some(items[items.len() - 1]))
                    },
                    Node::Variable(v) => {
                        (comp.get_value(v), None)
                    },
                    _ => panic!("Invalid record on {:?}", self)
                };
                let commit = *output_type == OutputType::Commit;
                let ref val = **value;
                let (a, v) = match (attr, val) {
                    (None, &Node::Tag(t)) => { (comp.interner.string("tag"), comp.interner.string(t)) },
                    (Some(attr), v) => {
                        (comp.interner.string(attr), v.compile(comp).unwrap())
                    },
                    (None, &Node::OutputRecord(..)) => {
                        match op {
                            &"<-" => {
                                val.compile(comp);
                                (Field::Value(0), Field::Value(0))
                            }
                            _ => panic!("Invalid {:?}", self)
                        }
                    }
                    _ => { panic!("Invalid {:?}", self) }
                };
                if a != Field::Value(0) && v != Field::Value(0) {
                    comp.constraints.push(Constraint::Insert{e:reg, a, v, commit});
                }
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
            &Node::Project(ref values) => {
                let registers = values.iter()
                                      .map(|v| v.compile(comp))
                                      .filter(|v| if let &Some(Field::Register(_)) = v { true } else { false })
                                      .map(|v| if let Some(Field::Register(reg)) = v { reg } else { panic!() })
                                      .collect();
                comp.constraints.push(Constraint::Project {registers});
                None
            },
            &Node::Watch(ref name, ref values) => {
                let registers = values.iter()
                                      .map(|v| v.compile(comp))
                                      .filter(|v| if let &Some(Field::Register(_)) = v { true } else { false })
                                      .map(|v| if let Some(Field::Register(reg)) = v { reg } else { panic!() })
                                      .collect();
                comp.constraints.push(Constraint::Watch {name:name.to_string(), registers});
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
    provided_registers: HashSet<Field>,
    interner: &'a mut Interner,
    constraints: Vec<Constraint>,
    equalities: Vec<(Field, Field)>,
    id: usize,
    reg_count: usize,
}

impl<'a> Compilation<'a> {
    pub fn new(interner: &'a mut Interner) -> Compilation<'a> {
        Compilation { vars:HashMap::new(), var_values:HashMap::new(), provided_registers:HashSet::new(), interner, constraints:vec![], equalities:vec![], id:0, reg_count:0 }
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

    pub fn provide(&mut self, reg:Field) {
        self.provided_registers.insert(reg);
    }

    pub fn is_provided(&mut self, name:&str) -> bool {
        let reg = self.get_register(name);
        self.provided_registers.contains(&reg)
    }
}

named!(pub space, eat_separator!(&b" \t\n\r,"[..]));

#[macro_export]
macro_rules! sp (
  ($i:expr, $($args:tt)*) => (
    {
      sep!($i, space, $($args)*)
    }
  )
);

named!(identifier<&str>, map_res!(is_not_s!("#\\.,()[]{}:=\"|; \r\n\t"), str::from_utf8));

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
       sp!(alt_complete!(
               number |
               string |
               record_function |
               record_reference |
               delimited!(tag!("("), expr, tag!(")"))
               )));

named!(expr<Node<'a>>,
       sp!(alt_complete!(
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
           op: sp!(alt_complete!(tag!(">=") | tag!("<=") | tag!("!=") | tag!("<") | tag!(">") | tag!("contains") | tag!("!contains"))) >>
           right: expr >>
           (Node::AttributeInequality{attribute, right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(record_set<Node<'a>>,
       do_parse!(
           records: many1!(sp!(record)) >>
           (Node::RecordSet(records))));

named!(attribute_equality<Node<'a>>,
       do_parse!(
           attr: identifier >>
           sp!(alt_complete!(tag!(":") | tag!("="))) >>
           value: alt_complete!(record_set | expr) >>
           (Node::AttributeEquality(attr, Box::new(value)))));

named!(attribute<Node<'a>>,
       sp!(alt_complete!(
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
           op: sp!(alt_complete!(tag!(">=") | tag!("<=") | tag!("!=") | tag!("<") | tag!(">") | tag!("contains") | tag!("!contains"))) >>
           right: expr >>
           (Node::Inequality{left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(infix_addition<Node<'a>>,
       do_parse!(
           left: alt_complete!(infix_multiplication | value) >>
           op: sp!(alt_complete!(tag!("+") | tag!("-"))) >>
           right: expr >>
           (Node::Infix{result:None, left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(infix_multiplication<Node<'a>>,
       do_parse!(
           left: value >>
           op: sp!(alt_complete!(tag!("*") | tag!("/"))) >>
           right: alt_complete!(infix_multiplication | value) >>
           (Node::Infix{result:None, left:Box::new(left), right:Box::new(right), op:str::from_utf8(op).unwrap()})));

named!(record_function<Node<'a>>,
       do_parse!(
          op: identifier >>
          tag!("[") >>
          params: many0!(alt_complete!(
                    attribute_equality |
                    identifier => { |v:&'a str| Node::Attribute(v) })) >>
          tag!("]") >>
          (Node::RecordFunction {result: None, op, params})));

named!(equality<Node<'a>>,
       do_parse!(
           left: expr >>
           op: sp!(tag!("=")) >>
           right: alt_complete!(expr | record) >>
           (Node::Equality {left:Box::new(left), right:Box::new(right)})));

named_args!(output_record_set<'a>(output_type:OutputType) <Node<'a>>,
       do_parse!(
           records: many1!(sp!(apply!(output_record, output_type))) >>
           (Node::RecordSet(records))));

named_args!(output_attribute_equality<'a>(output_type:OutputType) <Node<'a>>,
       do_parse!(
           attr: identifier >>
           sp!(alt_complete!(tag!(":") | tag!("="))) >>
           value: alt_complete!(apply!(output_record_set, output_type) | expr) >>
           (Node::AttributeEquality(attr, Box::new(value)))));

named_args!(output_attribute<'a>(output_type:OutputType) <Node<'a>>,
       sp!(alt_complete!(
               hashtag |
               apply!(output_attribute_equality, output_type) |
               tag!("|") => { |v:&[u8]| Node::Pipe } |
               identifier => { |v:&'a str| Node::Attribute(v) })));

named_args!(output_record<'a>(output_type:OutputType) <Node<'a>>,
       do_parse!(
           tag!("[") >>
           attrs: many0!(apply!(output_attribute, output_type)) >>
           tag!("]") >>
           (Node::OutputRecord(None, attrs, output_type))));

named!(attribute_access<Node<'a>>,
       do_parse!(start: identifier >>
                 rest: many1!(pair!(tag!("."), identifier)) >>
                 ({
                     let mut items = vec![start];
                     for (_, v) in rest {
                         items.push(v);
                     }
                     Node::AttributeAccess(items)
                 })));

named!(record_reference<Node<'a>>,
       sp!(alt_complete!(
               attribute_access |
               identifier => { |v:&'a str| Node::Variable(v) })));

named!(mutating_attribute_access<Node<'a>>,
       do_parse!(start: identifier >>
                 rest: many1!(pair!(tag!("."), identifier)) >>
                 ({
                     let mut items = vec![start];
                     for (_, v) in rest {
                         items.push(v);
                     }
                     Node::MutatingAttributeAccess(items)
                 })));

named!(mutating_record_reference<Node<'a>>,
       sp!(alt_complete!(
               mutating_attribute_access |
               identifier => { |v:&'a str| Node::Variable(v) })));

named!(bind_update<Node<'a>>,
       do_parse!(
           record: mutating_record_reference >>
           op: sp!(alt_complete!(tag!("+=") | tag!("<-"))) >>
           value: alt_complete!(apply!(output_record, OutputType::Bind) | expr | hashtag) >>
           (Node::RecordUpdate{ record: Box::new(record), op: str::from_utf8(op).unwrap(), value: Box::new(value), output_type:OutputType::Bind })));

named!(commit_update<Node<'a>>,
       do_parse!(
           record: mutating_record_reference >>
           op: sp!(alt_complete!(tag!(":=") | tag!("+=") | tag!("-=") | tag!("<-"))) >>
           value: alt_complete!(apply!(output_record, OutputType::Commit) | expr | hashtag) >>
           (Node::RecordUpdate{ record: Box::new(record), op: str::from_utf8(op).unwrap(), value: Box::new(value), output_type:OutputType::Commit })));


named!(search_section<Node<'a>>,
       do_parse!(
           sp!(tag!("search")) >>
           items: many0!(sp!(alt_complete!(
                            inequality |
                            record |
                            equality
                        ))) >>
           (Node::Search(items))));

named!(bind_section<Node<'a>>,
       do_parse!(
           sp!(tag!("bind")) >>
           items: many1!(sp!(alt_complete!(
                       apply!(output_record, OutputType::Bind) |
                       complete!(bind_update)
                       ))) >>
           (Node::Bind(items))));

named!(commit_section<Node<'a>>,
       do_parse!(
           sp!(tag!("commit")) >>
           items: many1!(sp!(alt_complete!(
                       apply!(output_record, OutputType::Commit) |
                       complete!(commit_update)
                       ))) >>
           (Node::Commit(items))));

named!(project_section<Node<'a>>,
       do_parse!(
           sp!(tag!("project")) >>
           items: sp!(delimited!(tag!("("), many1!(sp!(expr)) ,tag!(")"))) >>
           (Node::Project(items))));

named!(watch_section<Node<'a>>,
       do_parse!(
           sp!(tag!("watch")) >>
           watcher: sp!(identifier) >>
           items: sp!(delimited!(tag!("("), many1!(sp!(expr)) ,tag!(")"))) >>
           (Node::Watch(watcher, items))));

named!(block<Node<'a>>,
       sp!(do_parse!(
               search: opt!(search_section) >>
               update: alt_complete!( bind_section | commit_section | project_section | watch_section ) >>
               (Node::Block {search:Box::new(search), update:Box::new(update)}))));

named!(embedded_block<Node<'a>>,
       sp!(alt_complete!(
               delimited!(tag!("~~~"), block, tag!("~~~")) |
               delimited!(tag!("```"), block, tag!("```")))));

named!(surrounded_block<Option<Node<'a>>>,
       sp!(do_parse!(
               take_until_s!("~~~") >>
               block: opt!(embedded_block) >>
               (block))));

named!(test<Option<(&'a str, Node<'a>)>>,
       sp!(do_parse!(
               res: map_res!(take_until_s!("~~~"), str::from_utf8) >>
               tag!("~~~") >>
               block: sp!(block) >>
               sp!(tag!("~~~")) >>
               (Some((res, block))))));

named!(markdown<Node<'a>>,
       sp!(do_parse!(
               maybe_blocks: many1!(surrounded_block) >>
               ({
                   let mut blocks = vec![];
                   for block in maybe_blocks {
                       if let Some(v) = block {
                           blocks.push(v.clone());
                       }
                   }
                   Node::Doc { file:"foo.eve".to_string(), blocks}
               }))));

pub fn make_block(interner:&mut Interner, name:&str, content:&str) -> Block {
    let parsed = block(content.as_bytes());
    let mut comp = Compilation::new(interner);
    // println!("Parsed {:?}", parsed);
    match parsed {
        IResult::Done(_, mut block) => {
            block.unify(&mut comp);
            block.compile(&mut comp);
        }
        _ => { println!("Failed: {:?}", parsed); }
    }

    for c in comp.constraints.iter() {
        println!("{:?}", c);
    }

    Block::new(name, comp.constraints)
}

pub fn parse_file(program:&mut Program, path:&str) -> Vec<Block> {
    let mut file = File::open(path).expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read the file");
    let res = markdown(contents.as_bytes());
    if let IResult::Done(left, mut cur) = res {
        if let Node::Doc { ref mut blocks, .. } = cur {
            let interner = &mut program.interner;
            let mut program_blocks = vec![];
            let mut ix = 0;
            for block in blocks {
                // println!("\n\nBLOCK!");
                // println!("{:?}\n", block);
                ix += 1;
                let mut comp = Compilation::new(interner);
                block.unify(&mut comp);
                block.compile(&mut comp);
                // for c in comp.constraints.iter() {
                //     println!("{:?}", c);
                // }
                program_blocks.push(Block::new(&format!("{}|block|{}", path, ix), comp.constraints));
            }
            program_blocks
        } else {
            panic!("Got a non-doc parse??");
        }
    } else if let IResult::Error(Err::Position(err, pos)) = res {
        println!("ERROR: {:?}", err.description());
        println!("{:?}", str::from_utf8(pos));
        panic!("Failed to parse");
    } else {
        panic!("Failed to parse");
    }
}

#[test]
fn parser_coolness() {
    let mut program = Program::new();
    let blocks = parse_file(&mut program, "examples/test.eve");
    for block in blocks {
        program.raw_block(block);
    }
    // let res = commit_update(b"foo.hand += [asdf: 3]");
    // println!("{:?}", res);
    // let res = inequality(b"woah += 10");
    // if let IResult::Error(Err::Position(err, pos)) = res {
    //     println!("{:?}", err);
    //     println!("{:?}", err.description());
    //     println!("{:?}", str::from_utf8(pos));
    // };
    // let mut program = Program::new();
    // program.block("simple block", "search f = [#foo woah] bind [#bar baz: [#zomg]]");
    // let mut txn = Transaction::new();
    // txn.input(program.interner.number_id(1.0), program.interner.string_id("tag"), program.interner.string_id("foo"), 1);
    // txn.input(program.interner.number_id(1.0), program.interner.string_id("woah"), program.interner.number_id(1000.0), 1);
    // txn.exec(&mut program);
}
