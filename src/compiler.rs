extern crate time;
extern crate walkdir;
extern crate term_painter;

use self::term_painter::Color::*;
use self::term_painter::ToStyle;
use self::walkdir::WalkDir;
use combinators::{EMPTY_SPAN, ParseResult, ParseState, Span};
use error::{self, CompileError, report_errors};
use ops::{Block, Constraint, Field, Internable, Interner,
          make_aggregate, make_anti_scan, make_commit_lookup,
          make_filter, make_function, make_intermediate_insert,
          make_intermediate_scan, make_multi_function,
          make_remote_lookup, make_scan, register};
use parser::{block, embedded_blocks};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::collections::hash_map::RandomState;
use std::fs::{self, File};
use std::hash::Hash;
use std::io::prelude::*;

macro_rules! get_provided (
    ($comp:ident, $span:ident, $value:expr) => ({
        {
            let result = $comp.get_unified_register($value);
            match result {
                Provided::Yes(v) => { v }
                Provided::No(v) => {
                    $comp.error($span, error::Error::Unprovided($value.to_string()));
                    v
                },
            }
        }
    });
);

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum FunctionKind {
    Multi,
    Scalar,
    Sum,
    Sort,
    SortedSum,
    NeedleSort,
}

pub struct FunctionInfo {
    kind: FunctionKind,
    params: Vec<String>,
    outputs: Vec<String>,
}

pub enum ParamType {
    Param(usize),
    Output(usize),
    Invalid,
}

impl FunctionInfo {
    pub fn new(raw_params: Vec<&str>) -> FunctionInfo {
        let params = raw_params.iter()
                               .map(|s| s.to_string())
                               .collect();
        FunctionInfo {
            kind: FunctionKind::Scalar,
            params,
            outputs: vec![],
        }
    }

    pub fn multi(raw_params: Vec<&str>,
                 raw_outputs: Vec<&str>)
        -> FunctionInfo {
        let params = raw_params.iter()
                               .map(|s| s.to_string())
                               .collect();
        let outputs = raw_outputs.iter()
                                 .map(|s| s.to_string())
                                 .collect();
        FunctionInfo {
            kind: FunctionKind::Multi,
            params,
            outputs,
        }
    }

    pub fn aggregate(raw_params: Vec<&str>,
                     raw_outputs: Vec<&str>,
                     kind: FunctionKind)
        -> FunctionInfo {
        let params = raw_params.iter()
                               .map(|s| s.to_string())
                               .collect();
        let outputs = raw_outputs.iter()
                                 .map(|s| s.to_string())
                                 .collect();
        FunctionInfo {
            kind,
            params,
            outputs,
        }
    }


    pub fn get_index(&self, param: &str) -> ParamType {
        if let Some(v) =
            self.params
                .iter()
                .enumerate()
                .find(|&(_, t)| t == param)
        {
            ParamType::Param(v.0)
        } else if let Some(v) =
            self.outputs
                .iter()
                .enumerate()
                .find(|&(_, t)| t == param)
        {
            ParamType::Output(v.0)
        } else {
            ParamType::Invalid
        }
    }

    pub fn get_params(&self) -> &Vec<String> { &self.params }
}

lazy_static! {
    static ref DETERMINISTIC_STATE: RandomState = RandomState::new();
}

pub fn make_det_hash_map<K: Hash + Eq, V>() -> HashMap<K, V> {
    HashMap::with_hasher(DETERMINISTIC_STATE.clone())
}

pub fn make_det_hash_set<V: Hash + Eq>() -> HashSet<V> {
    HashSet::with_hasher(DETERMINISTIC_STATE.clone())
}

lazy_static! {
    static ref FUNCTION_INFO: HashMap<String, FunctionInfo> = {
        let mut m = make_det_hash_map();
        let mut info = make_det_hash_map();
        info.insert("degrees".to_string(), 0);
        m.insert("math/sin".to_string(), FunctionInfo::new(vec!["degrees"]));
        m.insert("math/cos".to_string(), FunctionInfo::new(vec!["degrees"]));
        m.insert("math/absolute".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("math/mod".to_string(), FunctionInfo::new(vec!["value", "by"]));
        m.insert("math/pow".to_string(), FunctionInfo::new(vec!["value", "exponent"]));
        m.insert("math/to-fixed".to_string(), FunctionInfo::new(vec!["value", "to"]));
        m.insert("math/to-hex".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("math/ceiling".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("math/floor".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("math/round".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("math/range".to_string(), FunctionInfo::multi(vec!["from", "to"], vec!["value"]));
        m.insert("random/number".to_string(), FunctionInfo::new(vec!["seed"]));
        m.insert("string/replace".to_string(), FunctionInfo::new(vec!["text", "replace", "with"]));
        m.insert("string/contains".to_string(), FunctionInfo::new(vec!["text", "substring"]));
        m.insert("string/lowercase".to_string(), FunctionInfo::new(vec!["text"]));
        m.insert("string/uppercase".to_string(), FunctionInfo::new(vec!["text"]));
        m.insert("string/length".to_string(), FunctionInfo::new(vec!["text"]));
        m.insert("string/substring".to_string(), FunctionInfo::new(vec!["text", "from", "to"]));
        m.insert("string/split".to_string(), FunctionInfo::multi(vec!["text", "by"], vec!["token", "index"]));
        m.insert("eve-internal/string/split-reverse".to_string(), FunctionInfo::multi(vec!["text", "by"], vec!["token", "index"]));
        m.insert("string/index-of".to_string(), FunctionInfo::multi(vec!["text", "substring"], vec!["index"]));
        m.insert("eve/type-of".to_string(), FunctionInfo::new(vec!["value"]));
        m.insert("gather/sum".to_string(), FunctionInfo::aggregate(vec!["value"], vec!["sum"], FunctionKind::Sum));
        m.insert("gather/average".to_string(), FunctionInfo::aggregate(vec!["value"], vec!["average"], FunctionKind::Sum));
        m.insert("gather/string-join".to_string(), FunctionInfo::aggregate(vec!["value", "separator"], vec!["string"], FunctionKind::SortedSum));
        m.insert("gather/count".to_string(), FunctionInfo::aggregate(vec![], vec!["count"], FunctionKind::Sum));
        m.insert("gather/top".to_string(), FunctionInfo::aggregate(vec!["limit"], vec!["top"], FunctionKind::Sort));
        m.insert("gather/bottom".to_string(), FunctionInfo::aggregate(vec!["limit"], vec!["bottom"], FunctionKind::Sort));
        m.insert("gather/next".to_string(), FunctionInfo::aggregate(vec![], vec!["*"], FunctionKind::NeedleSort));
        m.insert("gather/previous".to_string(), FunctionInfo::aggregate(vec![], vec!["*"], FunctionKind::NeedleSort));
        m
    };
}

pub fn get_function_info(op: &str) -> Option<&FunctionInfo> {
    return FUNCTION_INFO.get(op);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputType {
    Bind,
    Commit,
    Lookup,
}

#[derive(Debug, Clone)]
pub enum Node<'a> {
    Pipe,
    Pos(Span, Box<Node<'a>>),
    Integer(i32),
    Float(f32),
    RawString(&'a str),
    EmbeddedString(Option<String>, Vec<Node<'a>>),
    ExprSet(Vec<Node<'a>>),
    NoneValue,
    Tag(&'a str),
    Variable(&'a str),
    Identifier(&'a str),
    GeneratedVariable(String),
    Attribute(&'a str),
    AttributeEquality(&'a str, Box<Node<'a>>),
    AttributeInequality {
        attribute: &'a str,
        right: Box<Node<'a>>,
        op: &'a str,
    },
    AttributeAccess(Vec<&'a str>),
    MutatingAttributeAccess(Vec<&'a str>),
    Inequality {
        left: Box<Node<'a>>,
        right: Box<Node<'a>>,
        op: &'a str,
    },
    Equality {
        left: Box<Node<'a>>,
        right: Box<Node<'a>>,
    },
    Infix {
        result: Option<String>,
        left: Box<Node<'a>>,
        right: Box<Node<'a>>,
        op: &'a str,
    },
    Record(Option<String>, Vec<Node<'a>>),
    RecordSet(Vec<Node<'a>>),
    Lookup(Vec<Node<'a>>, OutputType),
    LookupCommit(Vec<Node<'a>>),
    LookupRemote(Vec<Node<'a>>, OutputType),
    RecordFunction {
        op: &'a str,
        params: Vec<Node<'a>>,
        outputs: Vec<Node<'a>>,
    },
    OutputRecord(Option<String>, Vec<Node<'a>>, OutputType),
    RecordUpdate {
        record: Box<Node<'a>>,
        value: Box<Node<'a>>,
        op: &'a str,
        output_type: OutputType,
    },
    Not(usize, Vec<Node<'a>>),
    IfBranch {
        sub_block_id: usize,
        exclusive: bool,
        result: Box<Node<'a>>,
        body: Vec<Node<'a>>,
    },
    If {
        sub_block_id: usize,
        exclusive: bool,
        outputs: Option<Vec<Node<'a>>>,
        branches: Vec<Node<'a>>,
    },
    Search(Vec<Node<'a>>),
    Bind(Vec<Node<'a>>),
    Commit(Vec<Node<'a>>),
    Project(Vec<Node<'a>>),
    Watch(&'a str, Vec<Node<'a>>),
    Block {
        code: &'a str,
        errors: Vec<ParseResult<'a, Node<'a>>>,
        search: Box<Option<Node<'a>>>,
        update: Box<Node<'a>>,
    },
    DisabledBlock(&'a str),
    Doc { file: String, blocks: Vec<Node<'a>> },
}

#[derive(Debug, Clone)]
pub enum SubBlock {
    Not(Compilation),
    Aggregate(Compilation,
              Vec<Field>,
              Vec<Field>,
              Vec<Field>,
              Vec<Field>,
              Vec<Field>,
              FunctionKind),
    AggregateScan(Compilation),
    IfBranch(Compilation, Vec<Field>),
    If(Compilation, Vec<Field>, bool),
}

impl SubBlock {
    pub fn get_mut_compilation(&mut self) -> &mut Compilation {
        match self {
            &mut SubBlock::Not(ref mut comp) => comp,
            &mut SubBlock::Aggregate(ref mut comp, ..) => comp,
            &mut SubBlock::AggregateScan(ref mut comp) => comp,
            &mut SubBlock::IfBranch(ref mut comp, ..) => comp,
            &mut SubBlock::If(ref mut comp, ..) => comp,
        }
    }
    pub fn get_output_registers(&self) -> Vec<Field> {
        match self {
            &SubBlock::Aggregate(_, ref outs, ..) => outs.clone(),
            &SubBlock::If(_, ref outs, ..) => outs.clone(),
            _ => vec![],
        }
    }

    pub fn get_all_registers(&self) -> Vec<Field> {
        match self {
            &SubBlock::Not(ref comp) => comp.get_all_registers(),
            &SubBlock::Aggregate(ref comp, ..) => {
                comp.get_all_registers()
            }
            &SubBlock::AggregateScan(ref comp) => {
                comp.get_all_registers()
            }
            &SubBlock::IfBranch(ref comp, ..) => {
                comp.get_all_registers()
            }
            &SubBlock::If(ref comp, ..) => comp.get_all_registers(),
        }
    }
}

impl<'a> Node<'a> {
    pub fn unwrap_pos(self) -> Node<'a> {
        match self {
            Node::Pos(_, node) => *node,
            _ => self,
        }
    }

    pub fn unwrap_ref_pos(&self) -> &Node<'a> {
        match self {
            &Node::Pos(_, box ref node) => node,
            _ => &self,
        }
    }

    pub fn to_pos_ref<'t>(&'t self,
                          cur_span: &'t Span)
        -> (&'t Span, &Node<'a>) {
        match self {
            &Node::Pos(ref span, box ref node) => (span, node),
            _ => (cur_span, &self),
        }
    }

    pub fn unify(&self, comp: &mut Compilation) {
        {
            let ref mut values: HashMap<Field, Field> =
                comp.var_values;
            let ref mut unified_registers: HashMap<Field,
                                                   Field> =
                comp.unified_registers;
            let ref mut provided = comp.provided_registers;
            for v in comp.vars.values() {
                let field = Field::Register(*v);
                values.insert(field, field);
                unified_registers.insert(field, field);
            }
            let mut changed = true;
            // go in rounds and try to unify everything
            while changed {
                changed = false;
                for &(l, r) in comp.equalities.iter() {
                    match (l, r) {
                        (Field::Register(l_reg),
                         Field::Register(r_reg)) => {
                            if l_reg < r_reg {
                                unified_registers.insert(r,
                                                         l.clone());
                            } else if r_reg < l_reg {
                                unified_registers.insert(l,
                                                         r.clone());
                            }
                        }
                        _ => {}
                    }

                    let left_value: Field =
                        if let Field::Register(_) = l {
                            values.entry(l).or_insert(l).clone()
                        } else {
                            l
                        };
                    let right_value: Field =
                        if let Field::Register(_) = r {
                            values.entry(r).or_insert(r).clone()
                        } else {
                            r
                        };
                    match (left_value, right_value) {
                        (Field::Register(l_reg),
                         Field::Register(r_reg)) => {
                            if provided.contains_key(&right_value) {
                                let v = provided.get(&right_value)
                                                .cloned()
                                                .unwrap();
                                provided.insert(l, v);
                            }
                            if provided.contains_key(&left_value) {
                                let v = provided.get(&left_value)
                                                .cloned()
                                                .unwrap();
                                provided.insert(r, v);
                            }
                            if l_reg < r_reg {
                                values.insert(r, left_value.clone());
                                unified_registers.insert(r, left_value.clone());
                                changed = true;
                            } else if r_reg < l_reg {
                                values.insert(l, right_value.clone());
                                unified_registers.insert(l, right_value.clone());
                                changed = true;
                            }
                        }
                        (Field::Register(_), other) => {
                            values.insert(l, other.clone());
                            provided.insert(l, true);
                            changed = true;
                        }
                        (other, Field::Register(_)) => {
                            values.insert(r, other.clone());
                            provided.insert(r, true);
                            changed = true;
                        }
                        (a, b) => {
                            if a != b {
                                panic!("Invalid equality {:?} != {:?}",
                                       a,
                                       b);
                            }
                        }
                    }
                }
            }
            comp.required_fields =
                comp.required_fields
                    .iter()
                    .map(|v| {
                    unified_registers.get(v)
                                     .unwrap()
                                     .clone()
                })
                    .collect();
        }

        for sub_block in comp.sub_blocks.iter_mut() {
            let sub_comp = sub_block.get_mut_compilation();
            // transfer values
            for (k, v) in comp.vars.iter() {
                match sub_comp.vars.entry(k.to_string()) {
                    Entry::Occupied(o) => {
                        let reg = o.get();
                        sub_comp.equalities
                                .push((Field::Register(*v),
                                       Field::Register(*reg)));
                    }
                    Entry::Vacant(o) => {
                        o.insert(*v);
                    }
                }
            }
            sub_comp.var_values = comp.var_values.clone();
            sub_comp.provided_registers
                    .extend(comp.provided_registers.iter());
            self.unify(sub_comp);
        }
    }

    pub fn gather_equalities(&mut self,
                             interner: &mut Interner,
                             cur_block: &mut Compilation)
        -> Option<Field> {
        match self {
            &mut Node::Pos(_, ref mut sub) => {
                sub.gather_equalities(interner, cur_block)
            }
            &mut Node::Pipe => None,
            &mut Node::DisabledBlock(_) => None,
            &mut Node::Tag(_) => None,
            &mut Node::Integer(v) => Some(interner.number(v as f32)),
            &mut Node::Float(v) => Some(interner.number(v)),
            &mut Node::RawString(v) => Some(interner.string(v)),
            &mut Node::Variable(v) => Some(cur_block.get_register(v)),
            &mut Node::GeneratedVariable(ref v) => {
                Some(cur_block.get_register(v))
            }
            &mut Node::NoneValue => None,
            &mut Node::Attribute(a) => {
                let reg = cur_block.get_register(a);
                if cur_block.mode == CompilationMode::Search {
                    cur_block.provide(reg, true);
                }
                Some(reg)
            }
            &mut Node::AttributeInequality {
                ref attribute,
                ref mut right,
                ..
            } => {
                let reg = cur_block.get_register(attribute);
                if cur_block.mode == CompilationMode::Search {
                    cur_block.provide(reg, true);
                }
                right.gather_equalities(interner, cur_block)
            }
            &mut Node::AttributeEquality(_, ref mut v) => {
                let result = v.gather_equalities(interner, cur_block);
                if let Some(reg) = result {
                    if cur_block.mode == CompilationMode::Search {
                        cur_block.provide(reg, true);
                    }
                }
                result
            }
            &mut Node::Inequality {
                ref mut left,
                ref mut right,
                ..
            } => {
                left.gather_equalities(interner, cur_block);
                right.gather_equalities(interner, cur_block);
                None
            }
            &mut Node::EmbeddedString(ref mut var, ref mut vs) => {
                for v in vs {
                    v.gather_equalities(interner, cur_block);
                }
                let var_name = format!("__eve_concat{}",
                                       cur_block.id);
                cur_block.id += 1;
                let reg = cur_block.get_register(&var_name);
                cur_block.provide(reg, true);
                *var = Some(var_name);
                Some(reg)

            }
            &mut Node::Equality {
                ref mut left,
                ref mut right,
            } => {
                let l = left.gather_equalities(interner, cur_block)
                            .unwrap();
                let r = right.gather_equalities(interner, cur_block)
                             .unwrap();
                cur_block.equalities.push((l, r));
                if cur_block.is_child {
                    if let Field::Register(_) = l {
                        cur_block.required_fields.push(l);
                    }
                    if let Field::Register(_) = r {
                        cur_block.required_fields.push(r);
                    }
                }
                None
            }
            &mut Node::ExprSet(ref mut items) => {
                for expr in items {
                    expr.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Infix {
                ref mut result,
                ref mut left,
                ref mut right,
                ..
            } => {
                left.gather_equalities(interner, cur_block);
                right.gather_equalities(interner, cur_block);
                let result_name = format!("__eve_infix{}",
                                          cur_block.id);
                cur_block.id += 1;
                let reg = cur_block.get_register(&result_name);
                cur_block.provide(reg, true);
                *result = Some(result_name);
                Some(reg)
            }
            &mut Node::RecordFunction {
                ref mut params,
                ref mut outputs,
                ..
            } => {
                for param in params.iter_mut() {
                    param.gather_equalities(interner, cur_block);
                }
                if outputs.len() == 0 {
                    let result_name = format!("__eve_infix{}",
                                              cur_block.id);
                    outputs.push(Node::GeneratedVariable(result_name));
                    cur_block.id += 1;
                }
                let first =
                    outputs[0]
                        .gather_equalities(interner, cur_block);
                if let Some(reg @ Field::Register(_)) = first {
                    cur_block.provide(reg, true);
                    cur_block.required_fields.push(reg);
                }
                for out in outputs[1..].iter_mut() {
                    let result =
                        out.gather_equalities(interner, cur_block);
                    if let Some(reg @ Field::Register(_)) = result {
                        cur_block.provide(reg, true);
                        cur_block.required_fields.push(reg);
                    }
                }
                first
            }
            &mut Node::Lookup(ref mut attrs, _) => {
                for attr in attrs {
                    attr.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::LookupCommit(ref mut attrs) => {
                for attr in attrs {
                    attr.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::LookupRemote(ref mut attrs, _) => {
                for attr in attrs {
                    attr.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::RecordSet(ref mut records) => {
                for record in records {
                    record.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Record(ref mut var, ref mut attrs) => {
                for attr in attrs {
                    attr.gather_equalities(interner, cur_block);
                }
                let var_name = format!("__eve_record{}",
                                       cur_block.id);
                cur_block.id += 1;
                let reg = cur_block.get_register(&var_name);
                cur_block.provide(reg, true);
                *var = Some(var_name);
                Some(reg)
            }
            &mut Node::OutputRecord(ref mut var,
                                    ref mut attrs,
                                    ..) => {
                for attr in attrs {
                    attr.gather_equalities(interner, cur_block);
                }
                let var_name = format!("__eve_output_record{}",
                                       cur_block.id);
                cur_block.id += 1;
                let reg = cur_block.get_register(&var_name);
                cur_block.provide(reg, false);
                *var = Some(var_name);
                Some(reg)
            }
            &mut Node::AttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                for item in items {
                    final_var.push_str("|");
                    final_var.push_str(item);
                }
                let reg = cur_block.get_register(&final_var);
                cur_block.provide(reg, true);
                Some(reg)
            }
            &mut Node::MutatingAttributeAccess(_) => None,
            &mut Node::RecordUpdate {
                ref mut record,
                ref op,
                ref mut value,
                ..
            } => {
                let left =
                    record.gather_equalities(interner, cur_block);
                let right =
                    value.gather_equalities(interner, cur_block);
                if op == &"<-" {
                    cur_block.provide(right.unwrap(), true);
                    if let &Node::MutatingAttributeAccess(..) =
                        record.unwrap_ref_pos()
                    {
                        // @NOTE: Compile will create a scan to resolve right, we only need to indicate that it exists.
                    } else {
                        cur_block.equalities
                                 .push((left.unwrap(),
                                        right.unwrap()));
                    }
                }
                None
            }
            &mut Node::Not(ref mut sub_id, ref mut items) => {
                let mut sub_block = Compilation::new_child(cur_block);
                for item in items {
                    item.gather_equalities(interner, &mut sub_block);
                }
                *sub_id = cur_block.sub_blocks.len();
                cur_block.sub_blocks
                         .push(SubBlock::Not(sub_block));
                None
            }
            &mut Node::IfBranch {
                ref mut sub_block_id,
                ref mut body,
                ref mut result,
                ..
            } => {
                let mut sub_block = Compilation::new_child(cur_block);
                for item in body {
                    item.gather_equalities(interner, &mut sub_block);
                }
                result.gather_equalities(interner, &mut sub_block);
                *sub_block_id = cur_block.sub_blocks.len();
                cur_block.sub_blocks
                         .push(SubBlock::IfBranch(sub_block, vec![]));
                None
            }
            &mut Node::If {
                ref mut sub_block_id,
                ref mut branches,
                ref mut outputs,
                exclusive,
                ..
            } => {
                let mut sub_block = Compilation::new_child(cur_block);
                if let &mut Some(ref mut outs) = outputs {
                    for out in outs {
                        let result =
                            out.gather_equalities(interner,
                                                  cur_block);
                        if let Some(reg) = result {
                            cur_block.provide(reg, true);
                        }
                    }
                }
                for branch in branches {
                    branch.gather_equalities(interner,
                                             &mut sub_block);
                }
                *sub_block_id = cur_block.sub_blocks.len();
                cur_block.sub_blocks
                         .push(SubBlock::If(sub_block,
                                            vec![],
                                            exclusive));
                None
            }
            &mut Node::Search(ref mut statements) => {
                cur_block.mode = CompilationMode::Search;
                for s in statements {
                    s.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Bind(ref mut statements) => {
                cur_block.mode = CompilationMode::Output;
                for s in statements {
                    s.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Commit(ref mut statements) => {
                cur_block.mode = CompilationMode::Output;
                for s in statements {
                    s.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Project(ref mut values) => {
                cur_block.mode = CompilationMode::Output;
                for v in values {
                    v.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Watch(_, ref mut values) => {
                cur_block.mode = CompilationMode::Output;
                for v in values {
                    v.gather_equalities(interner, cur_block);
                }
                None
            }
            &mut Node::Block {
                ref mut search,
                ref mut update,
                ..
            } => {
                if let Some(ref mut s) = **search {
                    s.gather_equalities(interner, cur_block);
                };
                update.gather_equalities(interner, cur_block);
                None
            }
            _ => panic!("Trying to gather equalities on {:?}", self),
        }
    }

    pub fn compile(&self,
                   interner: &mut Interner,
                   cur_block: &mut Compilation,
                   span: &Span)
        -> Option<Field> {
        match self {
            &Node::Pos(ref span, ref sub) => {
                sub.compile(interner, cur_block, span)
            }
            &Node::DisabledBlock(_) => None,
            &Node::Integer(v) => Some(interner.number(v as f32)),
            &Node::Float(v) => Some(interner.number(v)),
            &Node::RawString(v) => Some(interner.string(v)),
            &Node::Variable(v) => {
                Some(get_provided!(cur_block, span, v))
            }
            &Node::GeneratedVariable(ref v) => {
                Some(get_provided!(cur_block, span, v))
            }
            // &Node::AttributeEquality(a, ref v) => { v.compile(interner, comp, cur_block) },
            &Node::Equality {
                ref left,
                ref right,
            } => {
                left.compile(interner, cur_block, span);
                right.compile(interner, cur_block, span);
                None
            }
            &Node::AttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                let mut parent =
                    get_provided!(cur_block, span, items[0]);
                final_var.push_str("|");
                final_var.push_str(items[0]);
                for item in items[1..].iter() {
                    final_var.push_str("|");
                    final_var.push_str(item);
                    let reg = cur_block.get_register(&final_var);
                    cur_block.provide(reg, true);
                    let next =
                        get_provided!(cur_block, span, &final_var);
                    cur_block.constraints
                             .push(make_scan(parent,
                                             interner.string(item),
                                             next));
                    parent = next;
                }
                Some(parent)
            }
            &Node::MutatingAttributeAccess(ref items) => {
                let mut final_var = "attr_access".to_string();
                let mut parent =
                    get_provided!(cur_block, span, items[0]);
                if items.len() > 2 {
                    for item in items[1..items.len() - 1].iter() {
                        final_var.push_str("|");
                        final_var.push_str(item);
                        let reg = cur_block.get_register(&final_var);
                        cur_block.provide(reg, true);
                        let next = get_provided!(cur_block,
                                                 span,
                                                 &final_var);
                        cur_block.constraints.push(make_scan(parent, interner.string(item), next));
                        parent = next;
                    }
                }
                Some(parent)
            }
            &Node::Inequality {
                ref left,
                ref right,
                ref op,
            } => {
                let left_value =
                    left.compile(interner, cur_block, span);
                let right_value =
                    right.compile(interner, cur_block, span);
                match (left_value, right_value) {
                    (Some(l), Some(r)) => {
                        cur_block.constraints
                                 .push(make_filter(op, l, r));
                    }
                    _ => {
                        panic!("inequality without both a left and right: {:?} {} {:?}",
                               left,
                               op,
                               right)
                    }
                }
                right_value
            }
            &Node::EmbeddedString(ref var, ref vs) => {
                let resolved = vs.iter()
                                 .map(|v| {
                    v.compile(interner, cur_block, span)
                     .unwrap()
                })
                                 .collect();
                if let &Some(ref name) = var {
                    let mut out_reg = cur_block.get_register(name);
                    let out_value = cur_block.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        cur_block.constraints
                                 .push(make_filter("=",
                                                   out_reg,
                                                   out_value));
                    }
                    cur_block.constraints
                             .push(make_function("concat",
                                                 resolved,
                                                 out_reg));
                    Some(out_reg)
                } else {
                    panic!("Embedded string without a result assigned {:?}",
                           self);
                }

            }
            &Node::Infix {
                ref op,
                ref result,
                ref left,
                ref right,
            } => {
                let left_value =
                    left.compile(interner, cur_block, span)
                        .unwrap();
                let right_value =
                    right.compile(interner, cur_block, span)
                         .unwrap();
                if let &Some(ref name) = result {
                    let mut out_reg = cur_block.get_register(name);
                    let out_value = cur_block.get_value(name);
                    if let Field::Register(_) = out_value {
                        out_reg = out_value;
                    } else {
                        cur_block.constraints
                                 .push(make_filter("=",
                                                   out_reg,
                                                   out_value));
                    }
                    cur_block.constraints
                             .push(make_function(op,
                                                 vec![left_value,
                                                      right_value],
                                                 out_reg));
                    Some(out_reg)
                } else {
                    panic!("Infix without a result assigned {:?}",
                           self);
                }
            }
            &Node::RecordFunction {
                ref op,
                ref params,
                ref outputs,
            } => {
                let info = match FUNCTION_INFO.get(*op) {
                    Some(v) => v,
                    None => {
                        cur_block.error(span, error::Error::UnknownFunction(op.to_string()));
                        return Some(Field::Value(0));
                    }
                };
                let mut cur_outputs =
                    vec![
                        Field::Value(0);
                        cmp::max(outputs.len(), info.outputs.len())
                    ];
                let mut cur_params =
                    vec![Field::Value(0); info.params.len()];
                let mut group = vec![];
                let mut projection = vec![];
                let mut needle = vec![];
                for param in params {
                    let mut compiled_params = vec![];
                    let (_, unwrapped) = param.to_pos_ref(span);
                    match unwrapped {
                        &Node::Attribute(a) => {
                            compiled_params.push((a, cur_block.get_value(a)))
                        }
                        &Node::AttributeEquality(a, ref v) => {
                            let (local_pos, unwrapped) =
                                v.to_pos_ref(span);
                            if let &Node::ExprSet(ref items) =
                                unwrapped
                            {
                                for item in items {
                                    compiled_params.push((a, item.compile(interner, cur_block, local_pos).unwrap()))
                                }
                            } else {
                                compiled_params.push((a, v.compile(interner, cur_block, local_pos).unwrap()))
                            }
                        }
                        _ => {
                            panic!("invalid function param: {:?}",
                                   param)
                        }
                    };
                    for (a, v) in compiled_params {
                        match info.get_index(a) {
                            ParamType::Param(ix) => {
                                cur_params[ix] = v;
                            }
                            ParamType::Output(ix) => {
                                cur_outputs[ix] = v;
                            }
                            ParamType::Invalid => {
                                match (info.kind, a) {
                                    (FunctionKind::Sum, "per") |
                                    (FunctionKind::SortedSum,
                                     "per") |
                                    (FunctionKind::Sort, "per") |
                                    (FunctionKind::NeedleSort,
                                     "per") => group.push(v),
                                    (FunctionKind::Sum, "for") |
                                    (FunctionKind::SortedSum,
                                     "for") |
                                    (FunctionKind::Sort, "for") |
                                    (FunctionKind::NeedleSort,
                                     "for") => projection.push(v),
                                    (FunctionKind::NeedleSort,
                                     "from") => needle.push(v),
                                    _ => {
                                        cur_block.error(span, error::Error::UnknownFunctionParam(op.to_string(), a.to_string()));
                                    }
                                }
                            }
                        }
                    }
                }
                let compiled_outputs:Vec<Option<Field>> = outputs.iter().map(|output| output.compile(interner, cur_block, span).map(|x| cur_block.get_register_value(x))).collect();
                for (out_ix, mut attr_output) in
                    cur_outputs.iter_mut().enumerate()
                {
                    let cur_value = cur_block.get_register_value(attr_output.clone());
                    let maybe_output =
                        compiled_outputs.get(out_ix)
                                        .map(|x| x.unwrap());
                    match (&cur_value, maybe_output) {
                        (&Field::Value(0),
                         Some(Field::Register(_))) => {
                            *attr_output = maybe_output.unwrap();
                        }
                        (&Field::Value(0), Some(Field::Value(_))) => {
                            let result_name = format!("__eve_record_function_output{}",
                                                      cur_block.id);
                            let out_reg =
                                cur_block.get_register(&result_name);
                            cur_block.id += 1;
                            cur_block.constraints.push(make_filter("=", out_reg, maybe_output.unwrap()));
                            *attr_output = out_reg;
                        }
                        (&Field::Value(_),
                         Some(Field::Register(_))) => {
                            cur_block.constraints.push(make_filter("=", *attr_output, maybe_output.unwrap()));
                            *attr_output = maybe_output.unwrap();
                        }
                        (&Field::Register(_),
                         Some(Field::Value(_))) |
                        (&Field::Register(_),
                         Some(Field::Register(_))) => {
                            cur_block.constraints.push(make_filter("=", *attr_output, maybe_output.unwrap()));
                        }
                        (&Field::Value(x), None) => {
                            let result_name = format!("__eve_record_function_output{}",
                                                      cur_block.id);
                            let out_reg =
                                cur_block.get_register(&result_name);
                            cur_block.id += 1;
                            if x > 0 {
                                cur_block.constraints.push(make_filter("=", *attr_output, out_reg));
                            }
                            *attr_output = out_reg;
                        }
                        (&Field::Value(x), Some(Field::Value(z))) => {
                            if x != z {
                                panic!("Invalid constant equality in record function: {:?} != {:?}",
                                       x,
                                       z)
                            }
                            let result_name = format!("__eve_record_function_output{}",
                                                      cur_block.id);
                            let out_reg =
                                cur_block.get_register(&result_name);
                            cur_block.id += 1;
                            if x > 0 {
                                cur_block.constraints.push(make_filter("=", *attr_output, out_reg));
                            }
                            *attr_output = out_reg;
                        }
                        _ => {}
                    }
                }
                let final_result = Some(cur_outputs[0].clone());
                match info.kind {
                    FunctionKind::Multi => {
                        cur_block.constraints.push(make_multi_function(op, cur_params, cur_outputs));
                    }
                    FunctionKind::Sort | FunctionKind::Sum |
                    FunctionKind::SortedSum => {
                        let mut sub_block =
                            Compilation::new_child(cur_block);
                        let unified_output:Vec<Field> = cur_outputs.iter().map(|x| cur_block.get_unified(x)).collect();
                        sub_block.constraints.push(make_aggregate(op, group.clone(), projection.clone(), cur_params.clone(), unified_output.clone(), info.kind));
                        cur_block.sub_blocks.push(SubBlock::Aggregate(sub_block, group, projection, cur_params, vec![], unified_output, info.kind));
                    }
                    FunctionKind::NeedleSort => {
                        if needle.len() > 0 &&
                            needle.len() != projection.len()
                        {
                            cur_block.error(span, error::Error::InvalidNeedle);
                            return final_result;
                        }
                        let mut sub_block =
                            Compilation::new_child(cur_block);
                        let unified_output:Vec<Field> = cur_outputs.iter().map(|x| cur_block.get_unified(x)).collect();
                        let unified_needle:Vec<Field> = if needle.len() > 0 {
                            needle
                        } else {
                            projection.clone()
                        };
                        sub_block.constraints.push(make_aggregate(op, group.clone(), projection.clone(), cur_params.clone(), unified_output.clone(), info.kind));
                        cur_block.sub_blocks.push(SubBlock::Aggregate(sub_block, group, projection, cur_params, unified_needle, unified_output, info.kind));
                    }
                    FunctionKind::Scalar => {
                        cur_block.constraints
                                 .push(make_function(op,
                                                     cur_params,
                                                     cur_outputs[0]));
                    }
                }
                final_result
            }
            &Node::Lookup(ref attrs, output_type) => {
                let mut entity = None;
                let mut attribute = None;
                let mut value = None;
                let mut _type = None;

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    match unwrapped {
                        &Node::Attribute("entity") => {
                            entity = Some(get_provided!(cur_block,
                                                        local_span,
                                                        "entity"));
                        }
                        &Node::AttributeEquality("entity", ref v) => {
                            entity = v.compile(interner,
                                               cur_block,
                                               local_span);
                        }
                        _ => {}
                    }
                }

                if entity == None {
                    entity = cur_block.gen_var("eve_lookup");
                }

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    let (a, v) = match unwrapped {
                        &Node::Attribute(a) => {
                            (a,
                             Some(get_provided!(cur_block,
                                                local_span,
                                                a)))
                        }
                        &Node::AttributeEquality(a, ref v) => {
                            let (local_span, unwrapped) =
                                v.to_pos_ref(span);
                            let result = match unwrapped {
                                &Node::RecordSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                &Node::ExprSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                _ => {
                                    v.compile(interner,
                                              cur_block,
                                              local_span)
                                }
                            };
                            (a, result)
                        }
                        _ => {
                            panic!("Parse Error: Unrecognized node type in lookup attributes.")
                        }
                    };

                    // @FIXME: What do we do if there are multiple fields for a given a?
                    // Seems like that should be handled in gather_equalities, is it?
                    match a {
                        "entity" => {}
                        "attribute" => attribute = v,
                        "value" => value = v,
                        "type" => _type = v,
                        _ => {
                            panic!("Invalid lookup attribute '{}'. Lookup supports only entity, attribute, and value lookups.",
                                   a)
                        }
                    }
                }

                if attribute == None {
                    attribute = cur_block.gen_var("eve_lookup");
                }
                if value == None {
                    value = cur_block.gen_var("eve_lookup");
                }

                let constraint = match (output_type, _type) {
                    (OutputType::Lookup, _) => {
                        make_scan(entity.unwrap(),
                                  attribute.unwrap(),
                                  value.unwrap())
                    }
                    (OutputType::Bind, _) => {
                        Constraint::Insert {
                            e: entity.unwrap(),
                            a: attribute.unwrap(),
                            v: value.unwrap(),
                            commit: false,
                        }
                    }
                    (OutputType::Commit, None) => {
                        Constraint::Insert {
                            e: entity.unwrap(),
                            a: attribute.unwrap(),
                            v: value.unwrap(),
                            commit: true,
                        }
                    }
                    (OutputType::Commit, Some(Field::Value(v))) => {
                        match interner.get_value(v) {
                            &Internable::String(ref s)
                                if s == "add" => {
                                Constraint::Insert {
                                    e: entity.unwrap(),
                                    a: attribute.unwrap(),
                                    v: value.unwrap(),
                                    commit: true,
                                }
                            }
                            &Internable::String(ref s)
                                if s == "remove" => {
                                Constraint::Remove {
                                    e: entity.unwrap(),
                                    a: attribute.unwrap(),
                                    v: value.unwrap(),
                                }
                            }
                            _ => {
                                cur_block.error(span, error::Error::InvalidLookupType);
                                return None;
                            }
                        }
                    }
                    (OutputType::Commit,
                     Some(Field::Register(_))) => {
                        Constraint::DynamicCommit {
                            e: entity.unwrap(),
                            a: attribute.unwrap(),
                            v: value.unwrap(),
                            _type: _type.unwrap(),
                        }
                    }
                };
                cur_block.constraints.push(constraint);

                None
            }
            &Node::LookupCommit(ref attrs) => {
                let mut entity = None;
                let mut attribute = None;
                let mut value = None;

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    match unwrapped {
                        &Node::Attribute("entity") => {
                            entity = Some(get_provided!(cur_block,
                                                        local_span,
                                                        "entity"));
                        }
                        &Node::AttributeEquality("entity", ref v) => {
                            entity = v.compile(interner,
                                               cur_block,
                                               local_span);
                        }
                        _ => {}
                    }
                }

                if entity == None {
                    entity = cur_block.gen_var("eve_lookup");
                }

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    let (a, v) = match unwrapped {
                        &Node::Attribute(a) => {
                            (a,
                             Some(get_provided!(cur_block,
                                                local_span,
                                                a)))
                        }
                        &Node::AttributeEquality(a, ref v) => {
                            let (local_span, unwrapped) =
                                v.to_pos_ref(span);
                            let result = match unwrapped {
                                &Node::RecordSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                &Node::ExprSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                _ => {
                                    v.compile(interner,
                                              cur_block,
                                              local_span)
                                }
                            };
                            (a, result)
                        }
                        _ => {
                            panic!("Parse Error: Unrecognized node type in lookup attributes.")
                        }
                    };

                    // @FIXME: What do we do if there are multiple fields for a given a?
                    // Seems like that should be handled in gather_equalities, is it?
                    match a {
                        "entity" => {}
                        "attribute" => attribute = v,
                        "value" => value = v,
                        _ => {
                            panic!("Invalid lookup attribute '{}'. Lookup supports only entity, attribute, and value lookups.",
                                   a)
                        }
                    }
                }

                if attribute == None {
                    attribute = cur_block.gen_var("eve_lookup");
                }
                if value == None {
                    value = cur_block.gen_var("eve_lookup");
                }

                cur_block.constraints
                         .push(make_commit_lookup(entity.unwrap(),
                                                  attribute.unwrap(),
                                                  value.unwrap()));
                None
            }
            &Node::LookupRemote(ref attrs, output_type) => {
                let mut entity = None;
                let mut attribute = None;
                let mut value = None;
                let mut _for = None;
                let mut _type = None;
                let mut to = None;
                let mut from = None;

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    match unwrapped {
                        &Node::Attribute("entity") => {
                            entity = Some(get_provided!(cur_block,
                                                        local_span,
                                                        "entity"));
                        }
                        &Node::AttributeEquality("entity", ref v) => {
                            entity = v.compile(interner,
                                               cur_block,
                                               local_span);
                        }
                        _ => {}
                    }
                }

                if entity == None {
                    entity = cur_block.gen_var("eve_lookup");
                }

                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    let (a, v) = match unwrapped {
                        &Node::Attribute(a) => {
                            (a,
                             Some(get_provided!(cur_block,
                                                local_span,
                                                a)))
                        }
                        &Node::AttributeEquality(a, ref v) => {
                            let (local_span, unwrapped) =
                                v.to_pos_ref(span);
                            let result = match unwrapped {
                                &Node::RecordSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                &Node::ExprSet(..) => {
                                    panic!("Parse Error: We don't currently support Record sets as function attributes.");
                                }
                                _ => {
                                    v.compile(interner,
                                              cur_block,
                                              local_span)
                                }
                            };
                            (a, result)
                        }
                        _ => {
                            panic!("Parse Error: Unrecognized node type in lookup attributes.")
                        }
                    };

                    // @FIXME: What do we do if there are multiple fields for a given a?
                    // Seems like that should be handled in gather_equalities, is it?
                    match a {
                        "entity" => {}
                        "attribute" => attribute = v,
                        "value" => value = v,
                        "to" => to = v,
                        "from" => from = v,
                        "for" => _for = v,
                        "type" => _type = v,
                        _ => {
                            panic!("Invalid lookup attribute '{}'. Lookup supports only entity, attribute, and value lookups.",
                                   a)
                        }
                    }
                }

                if attribute == None {
                    attribute = cur_block.gen_var("eve_lookup");
                }
                if value == None {
                    value = cur_block.gen_var("eve_lookup");
                }
                if to == None {
                    to = cur_block.gen_var("eve_lookup");
                }
                if from == None {
                    from = cur_block.gen_var("eve_lookup");
                }
                if _for == None {
                    _for = cur_block.gen_var("eve_lookup");
                }
                if _type == None {
                    _type = cur_block.gen_var("eve_lookup");
                }

                let constraint = match output_type {
                    OutputType::Lookup => {
                        make_remote_lookup(entity.unwrap(),
                                           attribute.unwrap(),
                                           value.unwrap(),
                                           _for.unwrap(),
                                           _type.unwrap(),
                                           from.unwrap(),
                                           to.unwrap())
                    }
                    OutputType::Commit => {
                        Constraint::Watch {
                            name: "eve/remote".to_string(),
                            registers: vec![to.unwrap(),
                                            _for.unwrap(),
                                            entity.unwrap(),
                                            attribute.unwrap(),
                                            value.unwrap(),
                                            Field::Value(0)],
                        }
                    }
                    OutputType::Bind => {
                        Constraint::Watch {
                            name: "eve/remote".to_string(),
                            registers: vec![to.unwrap(),
                                            _for.unwrap(),
                                            entity.unwrap(),
                                            attribute.unwrap(),
                                            value.unwrap(),
                                            Field::Value(1)],
                        }
                    }
                };
                cur_block.constraints.push(constraint);
                None
            }
            &Node::Record(ref var, ref attrs) => {
                let reg = if let &Some(ref name) = var {
                    get_provided!(cur_block, span, name)
                } else {
                    panic!("Record missing a var {:?}", var)
                };
                for attr in attrs {
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    let (a, v) =
                        match unwrapped {
                            &Node::Tag(t) => {
                                (interner.string("tag"),
                                 interner.string(t))
                            }
                            &Node::Attribute(a) => {
                                (interner.string(a),
                                 get_provided!(cur_block,
                                               local_span,
                                               a))
                            }
                            &Node::AttributeEquality(a, ref v) => {
                                let result_a = interner.string(a);
                                let (local_span, unwrapped) =
                                    v.to_pos_ref(span);
                                let result =
                                    match unwrapped {
                                        &Node::RecordSet(ref records) => {
                                    for record in records[1..].iter() {
                                        let cur_v = record.compile(interner, cur_block, local_span).unwrap();
                                        cur_block.constraints.push(make_scan(reg, result_a, cur_v));
                                    }
                                    records[0].compile(interner, cur_block, local_span).unwrap()
                                }
                                        &Node::ExprSet(ref items) => {
                                            for value in items[1..]
                                                .iter()
                                            {
                                                let cur_v = value.compile(interner, cur_block, local_span).unwrap();
                                                cur_block.constraints.push(make_scan(reg, result_a, cur_v));
                                            }
                                            items[0]
                                                .compile(interner,
                                                         cur_block,
                                                         local_span)
                                                .unwrap()
                                        }
                                        _ => {
                                            v.compile(interner,
                                                      cur_block,
                                                      local_span)
                                             .unwrap()
                                        }
                                    };
                                (result_a, result)
                            }
                            &Node::AttributeInequality {
                                ref attribute,
                                ref op,
                                ref right,
                            } => {
                                let reg = get_provided!(cur_block,
                                                        span,
                                                        attribute);
                                let right_value =
                                    right.compile(interner,
                                                  cur_block,
                                                  local_span);
                                match right_value {
                                    Some(r) => {
                                        cur_block.constraints.push(make_filter(op, reg, r));
                                    }
                                    _ => {
                                        panic!("inequality without both a left and right: {} {} {:?}",
                                               attribute,
                                               op,
                                               right)
                                    }
                                }
                                (interner.string(attribute), reg)
                            }
                            _ => panic!("TODO"),
                        };
                    cur_block.constraints
                             .push(make_scan(reg, a, v));
                }
                Some(reg)
            }
            &Node::OutputRecord(ref var,
                                ref attrs,
                                ref output_type) => {
                let (reg, needs_id) = if let &Some(ref name) = var {
                    let provided = cur_block.is_provided(name);
                    if !provided {
                        let reg = cur_block.get_register(name);
                        cur_block.provide(reg, true);
                    }
                    let unified =
                        get_provided!(cur_block, span, name);
                    cur_block.provide(unified, true);
                    (unified, !provided)
                } else {
                    panic!("Record missing a var {:?}", var)
                };
                let commit = *output_type == OutputType::Commit;
                let mut identity_contributing = true;
                let mut identity_attrs = vec![];
                for attr in attrs {
                    if let &Node::Pipe = attr.unwrap_ref_pos() {
                        identity_contributing = false;
                        continue;
                    }
                    let (local_span, unwrapped) =
                        attr.to_pos_ref(span);
                    let (a, v) =
                        match unwrapped {
                            &Node::Tag(t) => {
                                (interner.string("tag"),
                                 interner.string(t))
                            }
                            &Node::Attribute(a) => {
                                (interner.string(a),
                                 get_provided!(cur_block,
                                               local_span,
                                               a))
                            }
                            &Node::AttributeEquality(a, ref v) => {
                                let result_a = interner.string(a);
                                let (local_span, unwrapped) =
                                    v.to_pos_ref(span);
                                let result =
                                    match unwrapped {
                                        &Node::RecordSet(ref records) => {
                                    let auto_index = interner.string("eve-auto-index");
                                    for (ix, record) in records[1..].iter().enumerate() {
                                        let cur_v = record.compile(interner, cur_block, local_span).unwrap();
                                        cur_block.constraints.push(Constraint::Insert{e:cur_v, a:auto_index, v:interner.number((ix + 2) as f32), commit});
                                        cur_block.constraints.push(Constraint::Insert{e:reg, a:result_a, v:cur_v, commit});
                                    }
                                    let sub_record = records[0].compile(interner, cur_block, local_span).unwrap();
                                    if records.len() > 1 {
                                        cur_block.constraints.push(Constraint::Insert{e:sub_record, a:auto_index, v:interner.number(1 as f32), commit});
                                    }
                                    sub_record
                                }
                                        &Node::ExprSet(ref items) => {
                                            for value in items[1..]
                                                .iter()
                                            {
                                                let cur_v = value.compile(interner, cur_block, local_span).unwrap();
                                                cur_block.constraints.push(Constraint::Insert{e:reg, a:result_a, v:cur_v, commit});
                                            }
                                            items[0]
                                                .compile(interner,
                                                         cur_block,
                                                         local_span)
                                                .unwrap()
                                        }
                                        _ => {
                                            v.compile(interner,
                                                      cur_block,
                                                      local_span)
                                             .unwrap()
                                        }
                                    };

                                (result_a, result)
                            }
                            _ => panic!("TODO"),
                        };
                    if identity_contributing {
                        identity_attrs.push(v);
                    }
                    cur_block.constraints
                             .push(Constraint::Insert {
                                       e: reg,
                                       a,
                                       v,
                                       commit,
                                   });
                }
                if needs_id {
                    cur_block.constraints
                             .push(make_function("gen_id",
                                                 identity_attrs,
                                                 reg));
                }
                Some(reg)
            }
            &Node::RecordUpdate {
                ref record,
                ref op,
                ref value,
                ref output_type,
            } => {
                // @TODO: compile attribute access correctly
                let (local_span, unwrapped) = record.to_pos_ref(span);
                let (reg, attr) = match unwrapped {
                    &Node::MutatingAttributeAccess(ref items) => {
                        let parent =
                            record.compile(interner,
                                           cur_block,
                                           local_span);
                        (parent.unwrap(),
                         Some(items[items.len() - 1]))
                    }
                    &Node::Variable(v) => {
                        (get_provided!(cur_block, local_span, v),
                         None)
                    }
                    _ => panic!("Invalid record on {:?}", self),
                };
                let commit = *output_type == OutputType::Commit;
                let (local_span, val) = value.to_pos_ref(span);
                let mut avs = vec![];
                match (attr, val) {
                    (None, &Node::Tag(t)) => {
                        avs.push((interner.string("tag"),
                                  interner.string(t)))
                    }
                    (Some(attr), &Node::Tag(t)) => {
                        let me = cur_block.gen_var("tag_mutation")
                                          .unwrap();
                        cur_block.constraints.push(make_scan(reg, interner.string(attr), me));
                        cur_block.constraints
                                 .push(Constraint::Insert {
                                           e: me,
                                           a: interner.string("tag"),
                                           v: interner.string(t),
                                           commit,
                                       });
                    }
                    (None, &Node::NoneValue) => {
                        avs.push((Field::Value(0), Field::Value(0)))
                    }
                    (Some(attr), &Node::NoneValue) => {
                        avs.push((interner.string(attr),
                                  Field::Value(0)))
                    }
                    (Some(attr), &Node::ExprSet(ref nodes)) => {
                        for node in nodes {
                            avs.push((interner.string(attr),
                                      node.compile(interner,
                                                   cur_block,
                                                   local_span)
                                          .unwrap()))
                        }
                    }
                    (Some(attr), &Node::RecordSet(ref nodes)) => {
                        for node in nodes {
                            avs.push((interner.string(attr),
                                      node.compile(interner,
                                                   cur_block,
                                                   local_span)
                                          .unwrap()))
                        }
                    }
                    (Some(attr),
                     &Node::OutputRecord(Some(ref name), ..)) => {
                        match op {
                            &"<-" => {
                                let me = get_provided!(cur_block,
                                                       span,
                                                       name);
                                cur_block.constraints.push(make_scan(reg, interner.string(attr), me));
                                val.compile(interner,
                                            cur_block,
                                            local_span);
                            }
                            _ => {
                                avs.push((interner.string(attr),
                                          val.compile(interner,
                                                      cur_block,
                                                      local_span)
                                             .unwrap()));
                            }
                        }
                    }
                    (Some(attr), v) => {
                        avs.push((interner.string(attr),
                                  v.compile(interner,
                                            cur_block,
                                            local_span)
                                   .unwrap()))
                    }
                    // @TODO: this doesn't handle the case where you do
                    // foo.bar <- [#zomg a]
                    (None, &Node::OutputRecord(..)) => {
                        match op {
                            &"<-" => {
                                val.compile(interner,
                                            cur_block,
                                            local_span);
                            }
                            _ => panic!("Invalid {:?}", self),
                        }
                    }
                    _ => panic!("Invalid {:?}", self),
                };
                for (a, v) in avs {
                    match (*op, a, v) {
                        (":=", Field::Value(0), Field::Value(0)) => {
                            cur_block.constraints
                                     .push(Constraint::RemoveEntity {
                                               e: reg,
                                           });
                        }
                        (":=", _, Field::Value(0)) => {
                            cur_block.constraints.push(Constraint::RemoveAttribute {e:reg, a });
                        }
                        (":=", ..) => {
                            cur_block.constraints.push(Constraint::RemoveAttribute {e:reg, a });
                            cur_block.constraints
                                     .push(Constraint::Insert {
                                               e: reg,
                                               a,
                                               v,
                                               commit,
                                           });
                        }
                        (_, Field::Value(0), Field::Value(0)) => {}
                        ("+=", ..) => {
                            cur_block.constraints
                                     .push(Constraint::Insert {
                                               e: reg,
                                               a,
                                               v,
                                               commit,
                                           });
                        }
                        ("-=", ..) => {
                            cur_block.constraints
                                     .push(Constraint::Remove {
                                               e: reg,
                                               a,
                                               v,
                                           });
                        }
                        _ => {
                            panic!("Invalid record update {:?} {:?} {:?}",
                                   op,
                                   a,
                                   v)
                        }
                    }
                }
                Some(reg)
            }
            &Node::Not(sub_block_id, ref items) => {
                let sub_block = if let SubBlock::Not(ref mut sub) =
                    cur_block.sub_blocks[sub_block_id]
                {
                    sub
                } else {
                    panic!("Wrong SubBlock type for Not");
                };
                for item in items {
                    item.compile(interner, sub_block, span);
                }
                None
            }
            &Node::IfBranch {
                sub_block_id,
                ref body,
                ref result,
                ..
            } => {
                if let SubBlock::IfBranch(ref mut sub_block,
                                          ref mut result_fields) =
                    cur_block.sub_blocks[sub_block_id]
                {
                    for item in body {
                        item.compile(interner, sub_block, span);
                    }
                    let (local_span, unwrapped) =
                        result.to_pos_ref(span);
                    if let &Node::ExprSet(ref nodes) = unwrapped {
                        for node in nodes {
                            result_fields.push(node.compile(interner, sub_block, local_span).unwrap());
                        }
                    } else {
                        result_fields.push(result.compile(interner, sub_block, local_span).unwrap());
                    }
                } else {
                    panic!("Wrong SubBlock type for Not");
                };
                None
            }
            &Node::If {
                sub_block_id,
                ref branches,
                ref outputs,
                ..
            } => {
                let compiled_outputs =
                    if let &Some(ref outs) = outputs {
                        outs.iter().map(|cur| {
                        let value = cur.compile(interner, cur_block, span).map(|x| cur_block.get_register_value(x));
                        match value {
                            Some(val @ Field::Value(_)) => {
                                let result_name = format!("__eve_if_output{}", cur_block.id);
                                let out_reg = cur_block.get_register(&result_name);
                                cur_block.provide(out_reg, true);
                                cur_block.id += 1;
                                cur_block.constraints.push(make_filter("=", out_reg, val));
                                out_reg
                            },
                            Some(reg @ Field::Register(_)) => {
                                cur_block.provide(reg, true);
                                let cur_value = if let Some(val @ &Field::Value(_)) = cur_block.var_values.get(&reg) {
                                    *val
                                } else {
                                    reg
                                };
                                if let Field::Value(_) = cur_value {
                                    let result_name = format!("__eve_if_output{}", cur_block.id);
                                    let out_reg = cur_block.get_register(&result_name);
                                    cur_block.id += 1;
                                    cur_block.constraints.push(make_filter("=", out_reg, cur_value));
                                    out_reg
                                } else {
                                    reg
                                }
                            },
                            _ => { panic!("Non-value, non-register if output") }
                        }
                    }).collect()
                    } else {
                        vec![]
                    };
                if let SubBlock::If(ref mut sub_block,
                                    ref mut out_registers,
                                    ..) =
                    cur_block.sub_blocks[sub_block_id]
                {
                    out_registers.extend(compiled_outputs);
                    for branch in branches {
                        branch.compile(interner, sub_block, span);
                    }
                }
                None
            }
            &Node::Search(ref statements) => {
                for s in statements {
                    s.compile(interner, cur_block, span);
                }
                None
            }
            &Node::Bind(ref statements) => {
                for s in statements {
                    s.compile(interner, cur_block, span);
                }
                None
            }
            &Node::Commit(ref statements) => {
                for s in statements {
                    s.compile(interner, cur_block, span);
                }
                None
            }
            &Node::Project(ref values) => {
                let registers = values.iter()
                                      .map(|v| {
                    v.compile(interner, cur_block, span)
                })
                                      .filter(|v| {
                    if let &Some(Field::Register(_)) = v {
                        true
                    } else {
                        false
                    }
                })
                                      .map(|v| {
                    if let Some(Field::Register(reg)) = v {
                        reg
                    } else {
                        panic!()
                    }
                })
                                      .collect();
                cur_block.constraints
                         .push(Constraint::Project { registers });
                None
            }
            &Node::Watch(ref name, ref values) => {
                for value in values {
                    let (local_span, unwrapped) =
                        value.to_pos_ref(span);
                    if let &Node::ExprSet(ref items) = unwrapped {
                        let registers =
                            items.iter()
                                 .map(|v| {
                                v.compile(interner,
                                          cur_block,
                                          local_span)
                                 .unwrap()
                            })
                                 .collect();
                        cur_block.constraints
                                 .push(Constraint::Watch {
                                           name: name.to_string(),
                                           registers,
                                       });
                    }
                }
                None
            }
            &Node::Block {
                ref search,
                ref update,
                ref errors,
                ..
            } => {
                if errors.len() > 0 {
                    for error in errors {
                        cur_block.errors
                                 .push(error::from_parse_error(error))
                    }
                    return None;
                }

                if let Some(ref s) = **search {
                    s.compile(interner, cur_block, span);
                };
                update.compile(interner, cur_block, span);

                self.sub_blocks(interner, cur_block);
                None
            }
            _ => {
                panic!("Trying to compile something we don't know how to compile {:?}",
                       self)
            }
        }
    }

    pub fn sub_blocks(&self,
                      interner: &mut Interner,
                      parent: &mut Compilation) {
        // gather all the registers that we know about at the root
        let mut parent_registers: HashSet<Field> =
            make_det_hash_set();
        for constraint in parent.constraints.iter() {
            parent_registers.extend(constraint.get_registers()
                                              .iter());
        }
        for sub_block in parent.sub_blocks.iter() {
            parent_registers.extend(sub_block.get_output_registers()
                                             .iter());
        }

        let ref mut ancestor_constraints = parent.constraints;

        let mut block_to_inputs =
            vec![make_det_hash_set(); parent.sub_blocks.len()];
        // go through the sub blocks to determine what their inputs are and generate their
        // outputs
        for (ix, sub_block) in
            parent.sub_blocks.iter_mut().enumerate()
        {
            let mut sub_registers = make_det_hash_set();
            sub_registers.extend(sub_block.get_all_registers()
                                          .iter());
            block_to_inputs[ix]
                .extend(parent_registers.intersection(&sub_registers)
                                        .cloned());
            ancestor_constraints.push(self.sub_block_output(interner, sub_block, ix, &block_to_inputs[ix]));
        }
        // now do it again, but this time compile
        for (ix, sub_block) in
            parent.sub_blocks.iter_mut().enumerate()
        {
            self.compile_sub_block(interner,
                                   sub_block,
                                   ix,
                                   &block_to_inputs[ix],
                                   &ancestor_constraints);
        }

    }

    pub fn sub_block_output(&self,
                            interner: &mut Interner,
                            block: &mut SubBlock,
                            ix: usize,
                            inputs: &HashSet<Field>)
        -> Constraint {
        match block {
            &mut SubBlock::Not(ref mut cur_block) => {
                let block_name = cur_block.block_name.to_string();
                let tag_value =
                    interner.string(&format!("{}|sub_block|not|{}",
                                             block_name,
                                             ix));
                let mut key_attrs = vec![tag_value];
                key_attrs.extend(inputs.iter());
                make_anti_scan(key_attrs)
            }
            &mut SubBlock::Aggregate(ref mut cur_block,
                                     ref group,
                                     ref projection,
                                     _,
                                     ref needle,
                                     ref output,
                                     kind) => {
                let block_name = cur_block.block_name.to_string();
                let result_id = interner.string(&format!("{}|sub_block|aggregate_result|{}", block_name, ix));
                let mut result_key = vec![result_id];
                result_key.extend(group.iter());
                match kind {
                    FunctionKind::Sort => {
                        result_key.extend(projection.iter())
                    }
                    FunctionKind::NeedleSort => {
                        result_key.extend(needle.iter())
                    }
                    _ => {}
                }
                make_intermediate_scan(result_key, output.clone())
            }
            &mut SubBlock::AggregateScan(..) => {
                panic!("Tried directly compiling an aggregate scan")
            }
            &mut SubBlock::IfBranch(..) => {
                panic!("Tried directly compiling an if branch")
            }
            &mut SubBlock::If(ref mut cur_block,
                              ref output_registers,
                              ..) => {
                let block_name = cur_block.block_name.to_string();
                let if_id = interner.string(&format!("{}|sub_block|if|{}",
                                                     block_name,
                                                     ix));
                let mut parent_if_key = vec![if_id];
                parent_if_key.extend(inputs.iter());
                make_intermediate_scan(parent_if_key,
                                       output_registers.clone())
            }
        }

    }

    pub fn compile_sub_block(&self,
                             interner: &mut Interner,
                             block: &mut SubBlock,
                             ix: usize,
                             inputs: &HashSet<Field>,
                             ancestor_constraints: &Vec<Constraint>) {
        let output_constraint =
            self.sub_block_output(interner, block, ix, inputs);
        match block {
            &mut SubBlock::Not(ref mut cur_block) => {
                self.sub_blocks(interner, cur_block);
                let valid_ancestors =
                    ancestor_constraints.iter()
                                        .filter(
                        |x| *x != &output_constraint,
                    )
                                        .cloned()
                                        .collect();
                let mut related =
                    get_input_constraints(&inputs, &valid_ancestors);
                related.extend(cur_block.constraints.iter().cloned());
                let block_name = cur_block.block_name.to_string();
                let tag_value =
                    interner.string(&format!("{}|sub_block|not|{}",
                                             block_name,
                                             ix));
                let mut key_attrs = vec![tag_value];
                key_attrs.extend(inputs.iter());
                related.push(make_intermediate_insert(key_attrs,
                                                      vec![],
                                                      true));
                cur_block.constraints = related;
            }
            &mut SubBlock::Aggregate(ref mut cur_block,
                                     ref group,
                                     ref projection,
                                     ref params,
                                     ..) => {
                let block_name = cur_block.block_name.to_string();

                // generate the scan block
                let mut scan_block =
                    Compilation::new_child(cur_block);
                let valid_ancestors =
                    ancestor_constraints.iter()
                                        .filter(
                        |x| *x != &output_constraint,
                    )
                                        .cloned()
                                        .collect();
                let mut related = get_input_constraints_transitive(&inputs, &valid_ancestors);
                let scan_id = interner.string(&format!("{}|sub_block|aggregate_scan|{}", block_name, ix));
                let mut key_attrs = vec![scan_id.clone()];
                key_attrs.extend(group.iter());
                let mut value_attrs = projection.clone();
                value_attrs.extend(params.iter());
                related.push(make_intermediate_insert(key_attrs,
                                                      value_attrs,
                                                      false));
                scan_block.constraints = related;
                cur_block.sub_blocks
                         .push(SubBlock::AggregateScan(scan_block));

                // add the lookup for the intermediates generated by the scan block
                let aggregate_id = interner.string(&format!("{}|sub_block|aggregate|{}", block_name, ix));
                let result_id = interner.string(&format!("{}|sub_block|aggregate_result|{}", block_name, ix));
                let mut result_key = vec![result_id];
                result_key.extend(group.iter());
                let mut scan_key = vec![scan_id];
                scan_key.extend(group.iter());
                scan_key.extend(projection.iter());
                scan_key.extend(params.iter());
                if let Constraint::Aggregate {
                    ref mut output_key,
                    ref mut group,
                    ..
                } = cur_block.constraints[0]
                {
                    group.insert(0, aggregate_id);
                    output_key.extend(result_key.iter());
                } else {
                    panic!("Aggregate block with a non-aggregate constraint")
                }
                cur_block.constraints
                         .push(make_intermediate_scan(scan_key,
                                                      vec![]));
            }
            &mut SubBlock::AggregateScan(..) => {
                panic!("Tried directly compiling an aggregate scan")
            }
            &mut SubBlock::IfBranch(..) => {
                panic!("Tried directly compiling an if branch")
            }
            &mut SubBlock::If(ref mut cur_block, _, exclusive) => {
                // get related constraints for all the inputs
                let valid_ancestors =
                    ancestor_constraints.iter()
                                        .filter(
                        |x| *x != &output_constraint,
                    )
                                        .cloned()
                                        .collect();
                let related = get_input_constraints(&inputs,
                                                    &valid_ancestors);
                let block_name = cur_block.block_name.to_string();
                let if_id = interner.string(&format!("{}|sub_block|if|{}",
                                                     block_name,
                                                     ix));

                // fix up the blocks for each branch
                let num_branches = cur_block.sub_blocks.len();
                let branch_ids:Vec<Field> = (0..num_branches).map(|branch_ix| {
                    interner.string(&format!("{}|sub_block|if|{}|branch|{}", block_name, ix, branch_ix))
                }).collect();
                for (branch_ix, sub) in
                    cur_block.sub_blocks
                             .iter_mut()
                             .enumerate()
                {
                    if let &mut SubBlock::IfBranch(ref mut branch_block, ref output_fields) = sub {
                        // add the related constraints to each branch
                        branch_block.constraints.extend(related.iter().map(|v| v.clone()));
                        self.sub_blocks(interner, branch_block);
                        if exclusive {
                            // Add an intermediate
                            if branch_ix + 1 < num_branches {
                                let mut branch_key = vec![branch_ids[branch_ix]];
                                branch_key.extend(inputs.iter());
                                branch_block.constraints.push(make_intermediate_insert(branch_key, vec![], true));
                            }

                            for prev_branch in 0..branch_ix {
                                let mut key_attrs = vec![branch_ids[prev_branch]];
                                key_attrs.extend(inputs.iter());
                                branch_block.constraints.push(make_anti_scan(key_attrs));
                            }
                        }
                        let mut if_key = vec![if_id];
                        if_key.extend(inputs.iter());
                        branch_block.constraints.push(make_intermediate_insert(if_key, output_fields.clone(), false));
                    }
                }
            }
        }
    }
}

pub fn get_input_constraints(needles: &HashSet<Field>,
                             haystack: &Vec<Constraint>)
    -> Vec<Constraint> {
    let mut related = make_det_hash_set();
    for hay in haystack {
        let mut found = false;
        let outs = hay.get_output_registers();
        for out in outs.iter() {
            if needles.contains(out) {
                found = true;
            }
        }
        if found && !related.contains(hay) {
            related.insert(hay.clone());
        }
    }
    // we're going to transitively include things that help us filter this down, but we don't want
    // to include IntermediateScans because they can easily lead to dependency cycles. Since this
    // part is a conservative optimization that's fine. Any hard dependency on an intermediate scan
    // will have been handled above.
    let mut transitive_needles = needles.clone();
    let mut changed = true;
    while changed {
        changed = false;
        let start_size = related.len();
        for hay in haystack {
            if let &Constraint::IntermediateScan { .. } = hay {
                continue;
            }
            let mut found = false;
            let outs = hay.get_filtering_registers();
            for out in outs.iter() {
                if transitive_needles.contains(out) {
                    found = true;
                }
            }
            if found {
                for filtering in hay.get_filtering_registers() {
                    transitive_needles.insert(filtering);
                }
                if !related.contains(hay) {
                    related.insert(hay.clone());
                }
            }
        }
        if related.len() > start_size {
            changed = true;
        }
    }
    let results = related.drain()
                         .collect::<Vec<Constraint>>();
    results
}

pub fn get_input_constraints_transitive(needles: &HashSet<Field>,
                                        haystack: &Vec<Constraint>)
    -> Vec<Constraint> {
    let mut transitive_needles = needles.clone();
    let mut related = make_det_hash_set();
    let mut changed = true;
    while changed {
        changed = false;
        let start_size = related.len();
        for hay in haystack {
            let mut found = false;
            let outs = hay.get_filtering_registers();
            for out in outs.iter() {
                if transitive_needles.contains(out) {
                    found = true;
                }
            }
            if found {
                for filtering in hay.get_filtering_registers() {
                    transitive_needles.insert(filtering);
                }
                related.insert(hay.clone());
            }
        }
        if related.len() > start_size {
            changed = true;
        }
    }
    let results = related.drain()
                         .collect::<Vec<Constraint>>();
    results
}

#[derive(Debug, Clone)]
pub enum Provided {
    Yes(Field),
    No(Field),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CompilationMode {
    Search,
    Output,
}

#[derive(Debug, Clone)]
pub struct Compilation {
    mode: CompilationMode,
    block_name: String,
    vars: HashMap<String, usize>,
    var_values: HashMap<Field, Field>,
    unified_registers: HashMap<Field, Field>,
    provided_registers: HashMap<Field, bool>,
    equalities: Vec<(Field, Field)>,
    pub constraints: Vec<Constraint>,
    sub_blocks: Vec<SubBlock>,
    required_fields: Vec<Field>,
    is_child: bool,
    id: usize,
    errors: Vec<CompileError>,
}

impl Compilation {
    pub fn new(block_name: String) -> Compilation {
        Compilation {
            mode: CompilationMode::Search,
            vars: make_det_hash_map(),
            var_values: make_det_hash_map(),
            unified_registers: make_det_hash_map(),
            provided_registers: make_det_hash_map(),
            equalities: vec![],
            id: 0,
            block_name,
            constraints: vec![],
            sub_blocks: vec![],
            required_fields: vec![],
            is_child: false,
            errors: vec![],
        }
    }

    pub fn new_child(parent: &Compilation) -> Compilation {
        let mut child = Compilation::new(format!("{}|{}",
                                                 parent.block_name,
                                                 parent.sub_blocks
                                                       .len()));
        child.id = parent.id + 10000 +
            (1000 * parent.sub_blocks.len());
        child.is_child = true;
        child
    }

    pub fn gen_var(&mut self, prefix: &str) -> Option<Field> {
        let var_name = format!("__{}{}", prefix, self.id);
        self.id += 1;
        let reg = self.get_register(&var_name);
        Some(reg)
    }

    pub fn error(&mut self, span: &Span, error: error::Error) {
        self.errors.push(CompileError {
                             span: span.clone(),
                             error,
                         });
    }

    pub fn get_register(&mut self, name: &str) -> Field {
        let ref mut id = self.id;
        let ix = *self.vars
                      .entry(name.to_string())
                      .or_insert_with(|| {
            *id += 1;
            *id
        });
        register(ix)
    }

    pub fn get_unified(&mut self, reg: &Field) -> Field {
        match self.unified_registers.get(&reg) {
            Some(&Field::Register(cur)) => Field::Register(cur),
            _ => reg.clone(),
        }
    }

    pub fn get_unified_register(&mut self, name: &str) -> Provided {
        let reg = self.get_register(name);
        let unified = match self.unified_registers.get(&reg) {
            Some(&Field::Register(cur)) => Field::Register(cur),
            _ => reg.clone(),
        };
        if !self.provided_registers
                .contains_key(&reg)
        {
            Provided::No(unified)
        } else {
            Provided::Yes(unified)
        }
    }

    pub fn get_all_registers(&self) -> Vec<Field> {
        let mut results = self.required_fields.clone();
        for constraint in self.constraints.iter() {
            results.extend(constraint.get_registers().iter());
        }
        for sub_block in self.sub_blocks.iter() {
            results.extend(sub_block.get_all_registers().iter());
        }
        results
    }

    pub fn get_inputs(&self,
                      haystack: &Vec<Constraint>)
        -> HashSet<Field> {
        let mut regs = make_det_hash_set();
        let mut input_regs = make_det_hash_set();
        for needle in self.constraints.iter() {
            for reg in needle.get_registers() {
                regs.insert(reg);
            }
        }
        regs.extend(self.required_fields.iter());
        for hay in haystack {
            for out in hay.get_output_registers() {
                if regs.contains(&out) {
                    input_regs.insert(out);
                }
            }
        }
        input_regs
    }

    pub fn finalize(&mut self) {
        self.reassign_registers();
        let mut collapsed = make_det_hash_set();
        collapsed.extend(self.constraints.drain(..));
        self.constraints.extend(collapsed);
    }

    pub fn reassign_registers(&mut self) {
        let mut regs = make_det_hash_map();
        let ref var_values = self.var_values;
        let mut ix = 0;
        for c in self.constraints.iter() {
            for reg in c.get_registers() {
                if regs.contains_key(&reg) {
                    continue;
                }

                let val = match var_values.get(&reg) {
                    Some(field @ &Field::Value(_)) => field.clone(),
                    Some(field @ &Field::Register(_)) => {
                        regs.entry(field.clone())
                            .or_insert_with(|| {
                            let out = Field::Register(ix);
                            ix += 1;
                            out
                        })
                            .clone()
                    }
                    _ => {
                        let out = Field::Register(ix);
                        ix += 1;
                        out
                    }
                };
                regs.insert(reg, val);
            }
        }
        for c in self.constraints.iter_mut() {
            c.replace_registers(&regs);
        }
    }

    pub fn get_value(&mut self, name: &str) -> Field {
        let reg = self.get_register(name);
        let val = self.var_values
                      .entry(reg)
                      .or_insert(reg);
        val.clone()
    }

    pub fn get_register_value(&mut self, reg: Field) -> Field {
        let val = self.var_values
                      .entry(reg)
                      .or_insert(reg);
        val.clone()
    }

    pub fn provide(&mut self, reg: Field, definitively: bool) {
        self.provided_registers
            .insert(reg, definitively);
    }

    pub fn is_provided(&mut self, name: &str) -> bool {
        let reg = self.get_register(name);
        self.provided_registers
            .get(&reg)
            .cloned()
            .unwrap_or(false)
    }
}

pub fn make_block(interner: &mut Interner,
                  name: &str,
                  content: &str)
    -> Vec<Block> {
    let mut state = ParseState::new(content);
    let parsed = block(&mut state);
    let mut comp = Compilation::new(name.to_string());
    // println!("Parsed {:?}", parsed);
    match parsed {
        ParseResult::Ok(mut block) => {
            block.gather_equalities(interner, &mut comp);
            block.unify(&mut comp);
            block.compile(interner, &mut comp, &EMPTY_SPAN);
        }
        _ => {
            println!("Failed: {:?}", parsed);
        }
    }

    comp.finalize();
    // for c in comp.constraints.iter() {
    //     println!("{:?}", c);
    // }
    compilation_to_blocks(comp, interner, name, content, false)
}

pub fn compilation_to_blocks(mut comp: Compilation,
                             interner: &mut Interner,
                             path: &str,
                             source: &str,
                             debug: bool)
    -> Vec<Block> {
    let mut compilation_blocks = vec![];
    if comp.errors.len() > 0 {
        report_errors(&comp.errors, path, source);
        return compilation_blocks;
    }

    let block_name = &comp.block_name;

    let mut sub_ix = 0;
    let mut subs: Vec<&mut SubBlock> =
        comp.sub_blocks.iter_mut().collect();
    while subs.len() > 0 {
        let sub_name = format!("{}|sub_block|{}", block_name, sub_ix);
        let mut cur = subs.pop().unwrap();
        let mut sub_comp = cur.get_mut_compilation();
        if sub_comp.constraints.len() > 0 {
            sub_comp.finalize();
            if debug {
                println!("       SubBlock: {}", sub_name);
                for c in sub_comp.constraints.iter() {
                    println!("            {:?}", c);
                }
            }
            let interned_name = interner.string_id(&sub_name);
            let mut block = Block::new(interner,
                                       &sub_name,
                                       interned_name,
                                       sub_comp.constraints.clone());
            block.path = path.to_owned();
            compilation_blocks.push(block);
        }
        subs.extend(sub_comp.sub_blocks.iter_mut());
        sub_ix += 1;
    }
    let interned_name = interner.string_id(&block_name);
    let mut block = Block::new(interner,
                               &block_name,
                               interned_name,
                               comp.constraints);
    block.path = path.to_owned();
    compilation_blocks.push(block);
    compilation_blocks
}

pub fn parse_string(interner: &mut Interner,
                    content: &str,
                    path: &str,
                    debug: bool)
    -> Vec<Block> {
    let mut state = ParseState::new(content);
    let res = embedded_blocks(&mut state, path);
    if let ParseResult::Ok(mut cur) = res {
        if let Node::Doc { ref mut blocks, .. } = cur {
            let mut program_blocks = vec![];
            let mut ix = 0;
            for block in blocks {
                ix += 1;
                let block_name = format!("{}|block|{}", path, ix);
                let mut comp =
                    Compilation::new(block_name.to_string());
                block.gather_equalities(interner, &mut comp);
                block.unify(&mut comp);
                block.compile(interner, &mut comp, &EMPTY_SPAN);

                comp.finalize();
                if debug {
                    println!("---------------------- Block {} ---------------------------",
                             block_name);
                    if let &mut Node::Block { code, .. } = block {
                        println!("{}\n\n => \n", code);
                    }
                    for c in comp.constraints.iter() {
                        println!("   {:?}", c);
                    }
                }
                program_blocks.extend(compilation_to_blocks(comp, interner, path, content, debug));
            }
            program_blocks
        } else {
            panic!("Got a non-doc parse??");
        }
    } else {
        panic!("Failed to parse");
    }
}

pub fn parse_file(interner: &mut Interner,
                  path: &str,
                  report: bool,
                  debug: bool)
    -> Vec<Block> {
    let metadata = fs::metadata(path)
        .expect(&format!("Invalid path: {:?}", path));
    let mut paths = vec![];
    if metadata.is_file() {
        paths.push(path.to_string());
    } else if metadata.is_dir() {
        for entry in WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let ext =
                    entry.path()
                         .extension()
                         .map(|x| x.to_str().unwrap());
                match ext {
                    Some("eve") | Some("md") => {
                        paths.push(entry.path()
                                        .canonicalize()
                                        .unwrap()
                                        .to_str()
                                        .unwrap()
                                        .to_string());
                    }
                    _ => {}
                }
            }
        }
    }
    let mut blocks = vec![];
    for cur_path in paths {
        if report {
            println!("{} {}",
                     BrightCyan.paint("Compiling:"),
                     cur_path.replace("\\", "/"));
        }
        let mut file = File::open(&cur_path)
            .expect("Unable to open the file");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Unable to read the file");
        blocks.extend(parse_string(interner,
                                   &contents,
                                   &cur_path,
                                   debug)
                      .into_iter());
    }
    blocks
}

#[test]
pub fn parser_test() {
    let mut file = File::open("examples/test2.eve")
        .expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Unable to read the file");
    let mut state = ParseState::new(&contents);
    let x = embedded_blocks(&mut state, "test.eve");
    println!("{:?}", x);
}
