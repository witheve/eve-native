extern crate time;

use nom::{digit, alphanumeric, anychar, IResult, Err};
use std::str::{self, FromStr};
use std::collections::{HashMap, HashSet};
use ops::{Interner, Field, Constraint, register, Program, make_scan, make_anti_scan, make_intermediate_insert, make_intermediate_scan, make_filter, make_function, Transaction, Block, Internable, RawChange};
use std::error::Error;
use std::io::prelude::*;
use std::fs::File;
use watcher::{PrintWatcher, SystemTimerWatcher};


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
    ExprSet(Vec<Node<'a>>),
    NoneValue,
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
    Not(Vec<Node<'a>>),
    IfBranch { exclusive:bool, result:Box<Node<'a>>, body:Vec<Node<'a>> },
    If { exclusive:bool, outputs:Option<Vec<Node<'a>>>, branches:Vec<Node<'a>> },
    Search(Vec<Node<'a>>),
    Bind(Vec<Node<'a>>),
    Commit(Vec<Node<'a>>),
    Project(Vec<Node<'a>>),
    Watch(&'a str, Vec<Node<'a>>),
    Block{search:Box<Option<Node<'a>>>, update:Box<Node<'a>>},
    Doc { file:String, blocks:Vec<Node<'a>> }
}

#[derive(Debug, Clone)]
pub enum SubBlock {
    Not(BlockCompilation),
    IfBranch(BlockCompilation, Vec<Field>),
    If(BlockCompilation, Vec<Field>, bool),
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
            &mut Node::NoneValue => { None },
            &mut Node::Attribute(_) => { None },
            &mut Node::AttributeInequality {ref mut right, ..} => { right.gather_equalities(comp) },
            &mut Node::AttributeEquality(a, ref mut v) => { v.gather_equalities(comp) },
            &mut Node::Inequality {ref left, ref right, ref op} => {
                None
            },
            &mut Node::EmbeddedString(ref mut var, ref mut vs) => {
                for v in vs {
                    v.gather_equalities(comp);
                }
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
            &mut Node::ExprSet(ref mut items) => {
                for expr in items {
                    expr.gather_equalities(comp);
                }
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
            &mut Node::Not(ref mut items) => {
                for item in items {
                    item.gather_equalities(comp);
                };
                None
            },
            &mut Node::IfBranch {ref mut body, ref mut result, ..} => {
                for item in body {
                    item.gather_equalities(comp);
                };
                result.gather_equalities(comp);
                None
            },
            &mut Node::If {ref mut branches, ref mut outputs, ..} => {
                if let &mut Some(ref mut outs) = outputs {
                    for out in outs {
                        out.gather_equalities(comp);
                    };
                }
                for branch in branches {
                    branch.gather_equalities(comp);
                };
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

    pub fn compile(&self, comp:&mut Compilation, cur_block: &mut BlockCompilation) -> Option<Field> {
        match self {
            &Node::Integer(v) => { Some(comp.interner.number(v as f32)) }
            &Node::Float(v) => { Some(comp.interner.number(v)) },
            &Node::RawString(v) => { Some(comp.interner.string(v)) },
            &Node::Variable(v) => { Some(comp.get_value(v)) },
            // &Node::AttributeEquality(a, ref v) => { v.compile(comp, cur_block) },
            &Node::AttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                let mut parent = comp.get_value(items[0]);
                for item in items[1..].iter() {
                    final_var.push_str("|");
                    final_var.push_str(item);
                    let next = comp.get_value(&final_var.to_string());
                    cur_block.constraints.push(make_scan(parent, comp.interner.string(item), next));
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
                        cur_block.constraints.push(make_scan(parent, comp.interner.string(item), next));
                        parent = next;
                    }
                }
                Some(parent)
            },
            &Node::Inequality {ref left, ref right, ref op} => {
                let left_value = left.compile(comp, cur_block);
                let right_value = right.compile(comp, cur_block);
                match (left_value, right_value) {
                    (Some(l), Some(r)) => {
                        cur_block.constraints.push(make_filter(op, l, r));
                    },
                    _ => panic!("inequality without both a left and right: {:?} {} {:?}", left, op, right)
                }
                right_value
            },
            &Node::EmbeddedString(ref var, ref vs) => {
                let resolved = vs.iter().map(|v| v.compile(comp, cur_block).unwrap()).collect();
                if let &Some(ref name) = var {
                    let mut out_reg = comp.get_register(name);
                    let out_value = comp.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        cur_block.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    cur_block.constraints.push(make_function("concat", resolved, out_reg));
                    Some(out_reg)
                } else {
                    panic!("Embedded string without a result assigned {:?}", self);
                }

            },
            &Node::Infix { ref op, ref result, ref left, ref right } => {
                let left_value = left.compile(comp, cur_block).unwrap();
                let right_value = right.compile(comp, cur_block).unwrap();
                if let &Some(ref name) = result {
                    let mut out_reg = comp.get_register(name);
                    let out_value = comp.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        cur_block.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    cur_block.constraints.push(make_function(op, vec![left_value, right_value], out_reg));
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
                        cur_block.constraints.push(make_filter("=", out_reg, out_value));
                    }
                    let info = FunctionInfo.get(*op).unwrap();
                    let mut cur_params = vec![Field::Value(0); info.len() - 1];
                    for param in params {
                        let (a, v) = match param {
                            &Node::Attribute(a) => {
                                (a, comp.get_value(a))
                            }
                            &Node::AttributeEquality(a, ref v) => {
                                (a, v.compile(comp, cur_block).unwrap())
                            }
                            _ => { panic!("invalid function param: {:?}", param) }
                        };
                        cur_params.insert(info[a], v);
                    }
                    cur_block.constraints.push(make_function(op, cur_params, out_reg));
                    Some(out_reg)
                } else {
                    panic!("Function without a result assigned {:?}", self);
                }
            },
            &Node::Equality {ref left, ref right} => {
                left.compile(comp, cur_block);
                right.compile(comp, cur_block);
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
                            let result = match **v {
                                Node::RecordSet(ref records) => {
                                    for record in records[1..].iter() {
                                        let cur_v = record.compile(comp, cur_block).unwrap();
                                        cur_block.constraints.push(make_scan(reg, result_a, cur_v));
                                    }
                                    records[0].compile(comp, cur_block).unwrap()
                                },
                                Node::ExprSet(ref items) => {
                                    for value in items[1..].iter() {
                                        let cur_v = value.compile(comp, cur_block).unwrap();
                                        cur_block.constraints.push(make_scan(reg, result_a, cur_v));
                                    }
                                    items[0].compile(comp, cur_block).unwrap()
                                },
                                _ => v.compile(comp, cur_block).unwrap()
                            };
                            (result_a, result)
                        },
                        &Node::AttributeInequality {ref attribute, ref op, ref right } => {
                            let reg = comp.get_value(attribute);
                            let right_value = right.compile(comp, cur_block);
                            match right_value {
                                Some(r) => {
                                    cur_block.constraints.push(make_filter(op, reg, r));
                                },
                                _ => panic!("inequality without both a left and right: {} {} {:?}", attribute, op, right)
                            }
                            (comp.interner.string(attribute), reg)
                        },
                        _ => { panic!("TODO") }
                    };
                    cur_block.constraints.push(make_scan(reg, a, v));
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
                            let result = match **v {
                                Node::RecordSet(ref records) => {
                                    for record in records[1..].iter() {
                                        let cur_v = record.compile(comp, cur_block).unwrap();
                                        cur_block.constraints.push(Constraint::Insert{e:reg, a:result_a, v:cur_v, commit});
                                    }
                                    records[0].compile(comp, cur_block).unwrap()
                                },
                                Node::ExprSet(ref items) => {
                                    for value in items[1..].iter() {
                                        let cur_v = value.compile(comp, cur_block).unwrap();
                                        cur_block.constraints.push(Constraint::Insert{e:reg, a:result_a, v:cur_v, commit});
                                    }
                                    items[0].compile(comp, cur_block).unwrap()
                                },
                                _ => v.compile(comp, cur_block).unwrap()
                            };

                            (result_a, result)
                        },
                        _ => { panic!("TODO") }
                    };
                    if identity_contributing {
                        identity_attrs.push(v);
                    }
                    cur_block.constraints.push(Constraint::Insert{e:reg, a, v, commit});
                };
                if needs_id {
                    cur_block.constraints.push(make_function("gen_id", identity_attrs, reg));
                }
                Some(reg)
            },
            &Node::RecordUpdate {ref record, ref op, ref value, ref output_type} => {
                // @TODO: compile attribute access correctly
                let (reg, attr) = match **record {
                    Node::MutatingAttributeAccess(ref items) => {
                        let parent = record.compile(comp, cur_block);
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
                    (None, &Node::NoneValue) => { (Field::Value(0), Field::Value(0)) }
                    (Some(attr), &Node::NoneValue) => { (comp.interner.string(attr), Field::Value(0)) }
                    (Some(attr), v) => {
                        (comp.interner.string(attr), v.compile(comp, cur_block).unwrap())
                    },
                    // @TODO: this doesn't handle the case where you do
                    // foo.bar <- [#zomg a]
                    (None, &Node::OutputRecord(..)) => {
                        match op {
                            &"<-" => {
                                val.compile(comp, cur_block);
                                (Field::Value(0), Field::Value(0))
                            }
                            _ => panic!("Invalid {:?}", self)
                        }
                    }
                    _ => { panic!("Invalid {:?}", self) }
                };
                match (*op, a, v) {
                    (":=", Field::Value(0), Field::Value(0)) => {
                        cur_block.constraints.push(Constraint::RemoveEntity {e:reg });
                    },
                    (":=", _, Field::Value(0)) => {
                        cur_block.constraints.push(Constraint::RemoveAttribute {e:reg, a });
                    },
                    (":=", _, _) => {
                        cur_block.constraints.push(Constraint::RemoveAttribute {e:reg, a });
                        cur_block.constraints.push(Constraint::Insert {e:reg, a, v, commit});
                    },
                    (_, Field::Value(0), Field::Value(0)) => {  }
                    ("+=", _, _) => { cur_block.constraints.push(Constraint::Insert {e:reg, a, v, commit}); }
                    ("-=", _, _) => { cur_block.constraints.push(Constraint::Remove {e:reg, a, v }); }
                    _ => { panic!("Invalid record update {:?} {:?} {:?}", op, a, v) }
                }
                Some(reg)
            },
            &Node::Not(ref items) => {
                let mut sub_block = BlockCompilation::new();
                for item in items {
                    item.compile(comp, &mut sub_block);
                };
                cur_block.sub_blocks.push(SubBlock::Not(sub_block));
                None
            },
            &Node::IfBranch { ref body, ref result, ..} => {
                println!("  branch body: {:?}", body);
                let mut sub_block = BlockCompilation::new();
                for item in body {
                    item.compile(comp, &mut sub_block);
                };
                let mut result_fields = vec![];
                if let Node::ExprSet(ref nodes) = **result {
                    for node in nodes {
                        result_fields.push(node.compile(comp, &mut sub_block).unwrap());
                    }
                } else {
                    result_fields.push(result.compile(comp, &mut sub_block).unwrap());
                }
                cur_block.sub_blocks.push(SubBlock::IfBranch(sub_block, result_fields));
                None
            },
            &Node::If { ref branches, ref outputs, exclusive } => {
                let mut sub_block = BlockCompilation::new();
                for branch in branches {
                    branch.compile(comp, &mut sub_block);
                };
                let out_registers = if let &Some(ref outs) = outputs {
                    outs.iter().map(|cur| {
                        if let &Node::Variable(v) = cur {
                            comp.get_register(v)
                        } else {
                            panic!("Invalid output for If");
                        }
                    }).collect()
                } else {
                    vec![]
                };
                cur_block.sub_blocks.push(SubBlock::If(sub_block, out_registers, exclusive));
                None
            },
            &Node::Search(ref statements) => {
                for s in statements {
                    s.compile(comp, cur_block);
                };
                None
            },
            &Node::Bind(ref statements) => {
                for s in statements {
                    s.compile(comp, cur_block);
                };
                None
            },
            &Node::Commit(ref statements) => {
                for s in statements {
                    s.compile(comp, cur_block);
                };
                None
            },
            &Node::Project(ref values) => {
                let registers = values.iter()
                                      .map(|v| v.compile(comp, cur_block))
                                      .filter(|v| if let &Some(Field::Register(_)) = v { true } else { false })
                                      .map(|v| if let Some(Field::Register(reg)) = v { reg } else { panic!() })
                                      .collect();
                cur_block.constraints.push(Constraint::Project {registers});
                None
            },
            &Node::Watch(ref name, ref values) => {
                let registers = values.iter()
                                      .map(|v| v.compile(comp, cur_block))
                                      .filter(|v| if let &Some(Field::Register(_)) = v { true } else { false })
                                      .map(|v| if let Some(Field::Register(reg)) = v { reg } else { panic!() })
                                      .collect();
                cur_block.constraints.push(Constraint::Watch {name:name.to_string(), registers});
                None
            },
            &Node::Block{ref search, ref update} => {
                if let Some(ref s) = **search {
                    s.compile(comp, cur_block);
                };
                update.compile(comp, cur_block);
                for (ix, mut sub) in cur_block.sub_blocks.iter_mut().enumerate() {
                    self.sub_block(comp, &mut cur_block.constraints, ix, sub);
                }
                None
            },
            _ => panic!("Trying to compile something we don't know how to compile {:?}", self)
        }
    }

    pub fn sub_block(&self, comp:&mut Compilation, parent_constraints:&mut Vec<Constraint>, ix:usize, block:&mut SubBlock) {
        println!("SUB");
        match block {
            &mut SubBlock::Not(ref mut cur_block) => {
                let (mut related, inputs) = get_related_constraints(&cur_block.constraints, parent_constraints);
                let block_name = comp.block_name.to_string();
                let tag_value = comp.interner.string(&format!("{}|sub_block|not|{}", block_name, ix));
                let mut key_attrs = vec![tag_value];
                key_attrs.extend(inputs);
                parent_constraints.push(make_anti_scan(key_attrs.clone()));
                related.push(make_intermediate_insert(key_attrs, vec![], true));
                for c in related.iter() {
                    println!("    {:?}", c);
                }
                cur_block.constraints = related;
            }
            &mut SubBlock::IfBranch(ref mut cur_block, ref output_fields) => {
            }
            &mut SubBlock::If(ref mut cur_block, ref output_registers, exclusive) => {
                println!("Let's compile an If!");
                // find the inputs for all of the branches
                let mut all_inputs = HashSet::new();
                for sub in cur_block.sub_blocks.iter_mut() {
                    if let &mut SubBlock::IfBranch(ref mut branch_block, ..) = sub {
                        let (_, inputs) = get_related_constraints(&branch_block.constraints, parent_constraints);
                        all_inputs.extend(inputs);
                    }
                }
                // get related constraints for all the inputs
                let related = get_input_constraints(&all_inputs, parent_constraints);
                println!("    Inputs: {:?}", all_inputs);
                println!("    RELATED: ");
                for rel in related.iter() {
                   println!("        {:?}", rel);
                }
                let block_name = comp.block_name.to_string();
                let if_id = comp.interner.string(&format!("{}|sub_block|if|{}", block_name, ix));

                // add an intermediate scan to the parent for the results of the branches
                let mut parent_if_key = vec![if_id];
                parent_if_key.extend(all_inputs.iter());
                parent_if_key.extend(output_registers.iter());
                parent_constraints.push(make_intermediate_scan(parent_if_key, output_registers.clone()));

                // fix up the blocks for each branch
                for (branch_ix, sub) in cur_block.sub_blocks.iter_mut().enumerate() {
                    if let &mut SubBlock::IfBranch(ref mut branch_block, ref output_fields) = sub {
                        // add the related constraints to each branch
                        branch_block.constraints.extend(related.iter().map(|v| v.clone()));
                        if exclusive {
                            // Add an intermediate
                            let mut branch_key = vec![if_id];
                            branch_key.extend(all_inputs.iter());
                            branch_key.push(comp.interner.number(branch_ix as f32));
                            branch_block.constraints.push(make_intermediate_insert(branch_key, vec![], true));

                            for prev_branch in 0..branch_ix {
                                let mut key_attrs = vec![if_id];
                                key_attrs.extend(all_inputs.iter());
                                key_attrs.push(comp.interner.number(branch_ix as f32));
                                branch_block.constraints.push(make_anti_scan(key_attrs));
                            }
                        }
                        let mut if_key = vec![if_id];
                        if_key.extend(all_inputs.iter());
                        if_key.extend(output_fields.iter());
                        branch_block.constraints.push(make_intermediate_insert(if_key, output_fields.clone(), false));
                    }
                }
            }
        }
    }
}

pub fn get_related_constraints(needles:&Vec<Constraint>, haystack:&Vec<Constraint>) -> (Vec<Constraint>, HashSet<Field>) {
    let mut regs = HashSet::new();
    let mut input_regs = HashSet::new();
    let mut related = needles.clone();
    for needle in needles.iter() {
        for reg in needle.get_registers() {
            regs.insert(reg);
        }
    }
    for hay in haystack {
        let mut found = false;
        let outs = hay.get_output_registers();
        for out in outs.iter() {
            if regs.contains(out) {
                found = true;
                input_regs.insert(*out);
            }
        }
        if found {
            related.push(hay.clone());
        }
    }
    (related, input_regs)
}

pub fn get_input_constraints(needles:&HashSet<Field>, haystack:&Vec<Constraint>) -> Vec<Constraint> {
    let mut related = vec![];
    for hay in haystack {
        let mut found = false;
        let outs = hay.get_output_registers();
        for out in outs.iter() {
            if needles.contains(out) {
                found = true;
            }
        }
        if found {
            related.push(hay.clone());
        }
    }
    related
}


#[derive(Debug, Clone)]
pub struct BlockCompilation {
    constraints: Vec<Constraint>,
    sub_blocks: Vec<SubBlock>,
}

impl BlockCompilation {
    pub fn new() -> BlockCompilation {
        BlockCompilation { constraints:vec![], sub_blocks:vec![] }
    }
}

pub struct Compilation<'a> {
    block_name: String,
    vars: HashMap<String, usize>,
    var_values: HashMap<Field, Field>,
    provided_registers: HashSet<Field>,
    interner: &'a mut Interner,
    constraints: Vec<Constraint>,
    equalities: Vec<(Field, Field)>,
    staged_nodes: Vec<SubBlock>,
    id: usize,
    reg_count: usize,
}

impl<'a> Compilation<'a> {
    pub fn new(interner: &'a mut Interner, block_name:String) -> Compilation<'a> {
        Compilation { vars:HashMap::new(), var_values:HashMap::new(), provided_registers:HashSet::new(), interner, constraints:vec![], equalities:vec![], staged_nodes:vec![], id:0, reg_count:0, block_name }
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

pub fn reassign_registers(constraints: &mut Vec<Constraint>) {
    let mut regs = HashMap::new();
    let mut ix = 0;
    for c in constraints.iter() {
        for reg in c.get_registers() {
            regs.entry(reg).or_insert_with(|| {
                let out = Field::Register(ix);
                ix += 1;
                out
            });
        }
    }
    for c in constraints.iter_mut() {
        c.replace_registers(&regs);
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
               match (parts.len(), parts.get(0)) {
                   (1, Some(&Node::RawString(_))) => parts.pop().unwrap(),
                   _ => Node::EmbeddedString(None, parts)
               }
           })));

named!(variable<Node<'a>>,
       do_parse!(i: identifier >>
                 (Node::Variable(i))));

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

named!(expr_set<Node<'a>>,
       do_parse!(
           items: sp!(delimited!(tag!("("), many1!(sp!(expr)) ,tag!(")"))) >>
           (Node::ExprSet(items))));

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
           value: alt_complete!(record_set | expr | expr_set) >>
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

named_args!(output_record_set<'a>(output_type:OutputType) <Node<'this_is_probably_unique_i_hope_please>>,
       do_parse!(
           records: many1!(sp!(apply!(output_record, output_type))) >>
           (Node::RecordSet(records))));

named_args!(output_attribute_equality<'a>(output_type:OutputType) <Node<'this_is_probably_unique_i_hope_please>>,
       do_parse!(
           attr: identifier >>
           sp!(alt_complete!(tag!(":") | tag!("="))) >>
           value: alt_complete!(apply!(output_record_set, output_type) | expr | expr_set) >>
           (Node::AttributeEquality(attr, Box::new(value)))));

named_args!(output_attribute<'a>(output_type:OutputType) <Node<'this_is_probably_unique_i_hope_please>>,
       sp!(alt_complete!(
               hashtag |
               apply!(output_attribute_equality, output_type) |
               tag!("|") => { |v:&[u8]| Node::Pipe } |
               identifier => { |v:&'this_is_probably_unique_i_hope_please str| Node::Attribute(v) })));

named_args!(output_record<'a>(output_type:OutputType) <Node<'this_is_probably_unique_i_hope_please>>,
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
       sp!(alt_complete!(attribute_access | variable)));

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
       sp!(alt_complete!(mutating_attribute_access | variable)));

named!(bind_update<Node<'a>>,
       do_parse!(
           record: mutating_record_reference >>
           op: sp!(alt_complete!(tag!("+=") | tag!("<-"))) >>
           value: alt_complete!(apply!(output_record, OutputType::Bind) | expr | hashtag) >>
           (Node::RecordUpdate{ record: Box::new(record), op: str::from_utf8(op).unwrap(), value: Box::new(value), output_type:OutputType::Bind })));

named!(none_value<Node<'a>>,
       do_parse!(
           tag!("none") >>
           ( Node::NoneValue )));

named!(commit_update<Node<'a>>,
       do_parse!(
           record: mutating_record_reference >>
           op: sp!(alt_complete!(tag!(":=") | tag!("+=") | tag!("-=") | tag!("<-"))) >>
           value: alt_complete!(apply!(output_record, OutputType::Commit) | none_value | expr | hashtag) >>
           (Node::RecordUpdate{ record: Box::new(record), op: str::from_utf8(op).unwrap(), value: Box::new(value), output_type:OutputType::Commit })));

named_args!(output_equality<'a>(output_type:OutputType) <Node<'this_is_probably_unique_i_hope_please>>,
       do_parse!(
           left: identifier >>
           sp!(tag!("=")) >>
           right: apply!(output_record, output_type) >>
           (Node::Equality {left:Box::new(Node::Variable(left)), right:Box::new(right)})));

named!(not_form<Node<'a>>,
       do_parse!(
           sp!(tag!("not")) >>
           items: delimited!(tag!("("),
                             many0!(sp!(alt_complete!(
                                         inequality |
                                         record |
                                         equality
                                         ))),
                             tag!(")")) >>
           (Node::Not(items))));

named!(if_equality<Vec<Node<'a>>>,
       do_parse!(
           outputs: alt_complete!(variable => { |v| vec![v] } |
                                  delimited!(tag!("("), many1!(variable), tag!(")"))) >>
           sp!(tag!("=")) >>
           (outputs)));

named!(if_else_branch<Node<'a>>,
       alt_complete!(
           if_branch |
           do_parse!(
               sp!(tag!("else")) >>
               branch: if_branch >>
               ({
                   if let Node::IfBranch { ref mut exclusive, .. } = branch.clone() {
                       *exclusive = true;
                       branch
                   } else {
                       panic!("Invalid if branch");
                   }
               })) |
           do_parse!(
               sp!(tag!("else")) >>
               result: alt_complete!(expr | expr_set) >>
               (Node::IfBranch {exclusive:true, body:vec![], result:Box::new(result)}))));

named!(if_branch<Node<'a>>,
       do_parse!(
           sp!(tag!("if")) >>
           body: many0!(sp!(alt_complete!(
                       inequality |
                       record |
                       equality
                       ))) >>
           sp!(tag!("then")) >>
           result: alt_complete!(expr | expr_set) >>
           (Node::IfBranch {exclusive:false, body, result:Box::new(result)})
                ));

named!(if_expression<Node<'a>>,
       do_parse!(
           outputs: opt!(if_equality) >>
           start_branch: if_branch >>
           other_branches: many0!(if_else_branch) >>
           ({
               let exclusive = other_branches.iter().any(|b| {
                   if let &Node::IfBranch {exclusive, ..} = b {
                       exclusive
                   } else {
                       false
                   }
               });
               let mut branches = vec![start_branch];
               branches.extend(other_branches);
               Node::If {exclusive, outputs, branches}
           })));


named!(search_section<Node<'a>>,
       do_parse!(
           sp!(tag!("search")) >>
           items: many0!(sp!(alt_complete!(
                            not_form |
                            if_expression |
                            inequality |
                            record |
                            equality
                        ))) >>
           (Node::Search(items))));

named!(bind_section<Node<'a>>,
       do_parse!(
           sp!(tag!("bind")) >>
           items: many1!(sp!(alt_complete!(
                       apply!(output_equality, OutputType::Bind) |
                       apply!(output_record, OutputType::Bind) |
                       complete!(bind_update)
                       ))) >>
           (Node::Bind(items))));

named!(commit_section<Node<'a>>,
       do_parse!(
           sp!(tag!("commit")) >>
           items: many1!(sp!(alt_complete!(
                       apply!(output_equality, OutputType::Commit) |
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
               sp!(tag!("end")) >>
               (Node::Block {search:Box::new(search), update:Box::new(update)}))));

named!(maybe_block<Option<Node<'a>>>,
       alt_complete!(block => { |block| Some(block) } |
                     eof!() => { |_| None }));

named!(surrounded_block<Option<Node<'a>>>,
       do_parse!(
           res: many_till!(anychar, maybe_block) >>
           (res.1)));

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

pub fn make_block(interner:&mut Interner, name:&str, content:&str) -> Vec<Block> {
    let mut blocks = vec![];
    let parsed = block(content.as_bytes());
    let mut comp = Compilation::new(interner, name.to_string());
    let mut block_comp = BlockCompilation::new();
    // println!("Parsed {:?}", parsed);
    match parsed {
        IResult::Done(_, mut block) => {
            block.unify(&mut comp);
            block.compile(&mut comp, &mut block_comp);
        }
        _ => { println!("Failed: {:?}", parsed); }
    }

    reassign_registers(&mut block_comp.constraints);
    for c in block_comp.constraints.iter() {
        println!("{:?}", c);
    }
    let sub_ix = 0;
    let mut subs = block_comp.sub_blocks.clone();
    while subs.len() > 0 {
        let cur = subs.pop().unwrap();
        let mut sub_comp = match cur {
            SubBlock::Not(comp) => comp,
            SubBlock::IfBranch(comp,..) => comp,
            SubBlock::If(comp,..) => comp,
        };
        subs.extend(sub_comp.sub_blocks);
        if sub_comp.constraints.len() > 0 {
            reassign_registers(&mut sub_comp.constraints);
            println!("    SubBlock");
            for c in sub_comp.constraints.iter() {
                println!("        {:?}", c);
            }
            blocks.push(Block::new(&format!("block|{}|sub_block|{}", name, sub_ix), sub_comp.constraints));
        }
    }

    blocks.push(Block::new(name, block_comp.constraints));
    blocks
}

pub fn parse_string(program:&mut Program, content:&str, path:&str) -> Vec<Block> {
    let res = markdown(content.as_bytes());
    if let IResult::Done(left, mut cur) = res {
        if let Node::Doc { ref mut blocks, .. } = cur {
            let interner = &mut program.state.interner;
            let mut program_blocks = vec![];
            let mut ix = 0;
            for block in blocks {
                // println!("\n\nBLOCK!");
                // println!("{:?}\n", block);
                ix += 1;
                let block_name = format!("{}|block|{}", path, ix);
                let mut comp = Compilation::new(interner, block_name.to_string());
                let mut block_comp = BlockCompilation::new();
                block.unify(&mut comp);
                block.compile(&mut comp, &mut block_comp);
                reassign_registers(&mut block_comp.constraints);
                println!("Block");
                for c in block_comp.constraints.iter() {
                    println!("{:?}", c);
                }
                let sub_ix = 0;
                let mut subs = block_comp.sub_blocks.clone();
                while subs.len() > 0 {
                    let cur = subs.pop().unwrap();
                    let mut sub_comp = match cur {
                        SubBlock::Not(comp) => comp,
                        SubBlock::IfBranch(comp,..) => comp,
                        SubBlock::If(comp,..) => comp,
                    };
                    subs.extend(sub_comp.sub_blocks);
                    if sub_comp.constraints.len() > 0 {
                        reassign_registers(&mut sub_comp.constraints);
                        println!("    SubBlock");
                        for c in sub_comp.constraints.iter() {
                            println!("        {:?}", c);
                        }
                        program_blocks.push(Block::new(&format!("{}|sub_block|{}", block_name, sub_ix), sub_comp.constraints));
                    }
                }
                println!("");
                program_blocks.push(Block::new(&block_name, block_comp.constraints));
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

pub fn parse_file(program:&mut Program, path:&str) -> Vec<Block> {
    let mut file = File::open(path).expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read the file");
    parse_string(program, &contents, path)
}

#[test]
pub fn parser_test() {
    let mut file = File::open("examples/test2.eve").expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read the file");
    let x = markdown(contents.as_bytes());
    println!("{:?}", x);
}

