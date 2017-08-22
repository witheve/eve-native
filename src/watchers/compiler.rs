use super::super::ops::{make_scan, make_function, Constraint, PortableConstraint, Interned, Internable, Interner, Field, RunLoopMessage};
use super::super::compiler::{get_function_info};
use indexes::{WatchDiff};
use std::sync::mpsc::{Sender};
use super::Watcher;
use compiler::{Compilation, compilation_to_blocks};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Entry};

pub enum ConstraintParams {
    Scan(Interned, Interned, Interned),
    Output(Interned, Interned, Interned),
    RemoteOutput(Interned, Interned, Interned, Interned, Interned),
    Function(Interned, Interned, HashMap<String, Interned>),
    Variadic(Interned, Interned, Vec<Interned>),
    GenId(Interned, HashMap<Interned, Interned>),
}

pub fn args_to_vec(op:&str, args:&HashMap<String, Field>) -> Option<Vec<Field>> {
    if let Some(info) = get_function_info(op) {
        let mut params:Vec<Field> = vec![];
        for param in info.get_params() {
            if let Some(&Field::Value(val)) = args.get(param) {
                if val != 0 { params.push(Field::Value(val)); }
                else { return None; }
            } else { return None; }
        }
        return Some(params);
    }
    None
}

pub fn identity_to_vec(args:&HashMap<String, Field>) -> Vec<Field> {
    // @FIXME: needs attr to soon.
    let mut keys:Vec<String> = vec![];
    keys.extend(args.keys().cloned());
    keys.sort();
    keys.iter().map(|attr| *args.get(attr).unwrap()).collect()
}

//-------------------------------------------------------------------------
// Compiler Watcher
//-------------------------------------------------------------------------

pub struct CompilerWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
    remote: bool,
    variable_ix: usize,
    variables: HashMap<Interned, Field>,
    block_types: HashMap<Interned, Interned>,
    block_to_constraints: HashMap<Interned, Vec<Interned>>,
    constraint_to_params: HashMap<Interned, ConstraintParams>,
    constraints: HashMap<Interned, Constraint>,
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>, remote: bool) -> CompilerWatcher {
        CompilerWatcher{name: "eve/compiler".to_string(),
                        outgoing,
                        remote,
                        variable_ix: 0,
                        variables: HashMap::new(),
                        block_types: HashMap::new(),
                        block_to_constraints: HashMap::new(),
                        constraint_to_params: HashMap::new(),
                        constraints: HashMap::new()}
    }

    pub fn block_type(&self, block:Interned, interner:&mut Interner) -> Option<String> {
        if let Some(&kind) = self.block_types.get(&block) {
            if let &Internable::String(ref string) = interner.get_value(kind) {
                return Some(string.to_string());
            }
        }
        None
    }

    pub fn add_constraint(&mut self, id:Interned, block:Interned, damaged_constraints:&mut HashSet<Interned>, damaged_blocks:&mut HashSet<Interned>) {
        let constraints = self.block_to_constraints.entry(block).or_insert_with(|| vec![]);
        constraints.push(id);
        damaged_blocks.insert(block);
        damaged_constraints.insert(id);
    }

    pub fn update_variables(&mut self, interner:&mut Interner, diff:&WatchDiff) {
        let kind = interner.string_id("variable");
        for var in diff.adds.iter().filter(|v| kind == v[0]) {
            match self.variables.entry(var[1]) {
                Entry::Occupied(_) => {},
                Entry::Vacant(entry) => {
                    let ix = self.variable_ix;
                    self.variable_ix += 1;
                    entry.insert(Field::Register(ix));
                }
            };
        }
    }

    fn get_field(&self, value:Interned) -> Field {
        self.variables.get(&value).cloned().unwrap_or_else(|| Field::Value(value))
    }


    pub fn compile_constraint(&self, mut constraint:&ConstraintParams, interner:&Interner, block_type:Option<String>) -> Option<Constraint> {
        let commit = match block_type.as_ref().map(String::as_str) { Some("commit") => true, _ => false };
        match constraint {
            &ConstraintParams::Scan(e, a, v) => Some(make_scan(self.get_field(e), self.get_field(a), self.get_field(v))),
            &ConstraintParams::Output(e, a, v) => Some(Constraint::Insert{e: self.get_field(e), a: self.get_field(a), v: self.get_field(v), commit}),
            &ConstraintParams::RemoteOutput(label, e, a, v, to) => {
                let commit_type = match commit { true => 0, false => 1 };
                let registers = vec![to, label, e, a, v, commit_type].iter().map(|&x| self.get_field(x)).collect();
                Some(Constraint::Watch {name:"eve/remote".to_string(), registers})
            },
            &ConstraintParams::Function(name, output, ref args) => {
                let op = interner.get_string(name).expect("Unable to resolve name of function.");
                let arg_fields:HashMap<String, Field> = args.iter().map(|(key, value)| (key.to_owned(), self.get_field(*value))).collect();
                match args_to_vec(&op, &arg_fields) {
                    Some(params) => Some(make_function(&op, params, self.get_field(output))),
                    _ => None
                }
            },
            &ConstraintParams::Variadic(name, output, ref args) => {
                let op = interner.get_string(name).expect("Unable to resolve name of variadic.");
                let params:Vec<Field> = args.iter().map(|value| self.get_field(*value)).collect();
                Some(make_function(&op, params, self.get_field(output)))
            },
            &ConstraintParams::GenId(var, ref interned_attrs) => {
                let arg_fields = interned_attrs.iter().map(|(key, value)| {
                    (interner.get_string(*key).expect("Unable to resolve attribute."), self.get_field(*value))
                }).collect();
                Some(make_function("gen_id", identity_to_vec(&arg_fields), self.get_field(var)))
            },
            _ => unimplemented!()
        }
    }
}

impl Watcher for CompilerWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        let mut existing_blocks:HashSet<Interned> = HashSet::new();
        existing_blocks.extend(self.block_types.keys());

        let mut damaged_blocks:HashSet<Interned> = HashSet::new();
        let mut damaged_constraints:HashSet<Interned> = HashSet::new();

        for remove in diff.removes {
            if let &Internable::String(ref kind) = interner.get_value(remove[0]) {
                match (kind.as_ref(), &remove[1..]) {
                    ("block", &[block, _]) => {
                        self.block_types.remove(&block);
                        damaged_blocks.insert(block);
                    },
                    ("variable", _) => {},
                    ("argument", &[constraint, attribute, _]) => {
                        if let Some(constraint_params) = self.constraint_to_params.get_mut(&constraint) {
                            match constraint_params {
                                &mut ConstraintParams::Function(_, _, ref mut params) => {
                                    if let &Internable::String(ref string) = interner.get_value(attribute) {
                                        params.remove(string);
                                    }
                                },
                                &mut ConstraintParams::Variadic(_, _, ref mut params) => {
                                    let ix = Internable::to_number(interner.get_value(attribute)) as usize;
                                    params[ix] = 0;
                                },
                                _ => unreachable!()
                            }
                            damaged_constraints.insert(constraint);
                        }
                    },
                    ("identity", &[constraint, attribute, ..]) => {
                        if let Some(&mut ConstraintParams::GenId(_, ref mut identity)) = self.constraint_to_params.get_mut(&constraint) {
                            identity.remove(&attribute);
                            damaged_constraints.insert(constraint);
                        }
                    },

                    // Constraints
                    ("scan", &[id, block, ..]) |
                    ("output", &[id, block, ..]) |
                    ("function", &[id, block, ..]) |
                    ("variadic", &[id, block, ..]) |
                    ("gen-id", &[id, block, ..]) |
                    ("remote-output", &[id, block, ..]) => {
                        self.constraints.remove(&id).unwrap();
                        self.constraint_to_params.remove(&id).unwrap();
                        self.block_to_constraints.get_mut(&block).unwrap().remove_item(&id);
                        damaged_blocks.insert(block);
                        damaged_constraints.insert(id);
                    },

                    _ => println!("Found other remove '{}' {:?}", kind, remove)
                }
            }
        }

        for add in diff.adds.iter() {
            if let &Internable::String(ref kind) = interner.get_value(add[0]) {
                match (kind.as_ref(), &add[1..]) {
                    ("block", &[block, kind]) => {
                        match self.block_types.entry(block) {
                            Entry::Occupied(_) => panic!("Cannot compile block with multiple types."),
                            Entry::Vacant(entry) => { entry.insert(kind); }
                        }
                        damaged_blocks.insert(block);
                    },
                    ("variable", &[id, _]) => {
                        // @FIXME: This should probably damage blocks that use the id of this var as a value, but that's a weird lookup...
                        let ix = self.variable_ix;
                        self.variable_ix += 1;
                        self.variables.insert(id, Field::Register(ix));
                    },
                    ("argument", ..) => {},
                    ("identity", ..) => {},

                    // Constraints
                    ("scan", &[id, block, e, a, v]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::Scan(e, a, v));
                    },
                    ("output", &[id, block, e, a, v]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::Output(e, a, v));
                    },
                    ("function", &[id, block, op, output]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::Function(op, output, HashMap::new()));
                    },
                    ("variadic", &[id, block, op, output]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::Variadic(op, output, vec![]));
                    },
                    ("gen-id", &[id, block, variable]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::GenId(variable, HashMap::new()));
                    },
                    ("remote-output", &[id, block, label, e, a, v, to]) => {
                        self.add_constraint(id, block, &mut damaged_constraints, &mut damaged_blocks);
                        self.constraint_to_params.insert(id, ConstraintParams::RemoteOutput(label, e, a, v, to));
                    },

                    _ => println!("Found other add '{}' {:?}", kind, add)
                }
            }
        }

        // Fill in function and variadic parameters.
        for add in diff.adds {
            if let &Internable::String(ref kind) = interner.get_value(add[0]) {
                match (kind.as_ref(), &add[1..]) {
                    ("argument", &[constraint, attribute, value]) => {
                        match self.constraint_to_params.get_mut(&constraint).unwrap() {
                            &mut ConstraintParams::Function(_, _, ref mut params) => {
                                if let &Internable::String(ref string) = interner.get_value(attribute) {
                                    params.insert(string.to_string(), value);
                                }
                            },
                            &mut ConstraintParams::Variadic(_, _, ref mut params) => {
                                let ix = Internable::to_number(interner.get_value(attribute)) as usize;
                                params[ix] = value;
                            },
                            _ => panic!("Invalid constraint type to receive arguments")
                        }
                        damaged_constraints.insert(constraint);
                    },
                    ("identity", &[constraint, attribute, value]) => {
                        if let &mut ConstraintParams::GenId(_, ref mut identity) = self.constraint_to_params.get_mut(&constraint).unwrap() {
                            identity.insert(attribute, value);
                            damaged_constraints.insert(constraint);
                        } else {
                            unreachable!();
                        }
                    },
                    _ => {}
                }
            }
        }



        let mut removed_blocks = vec![];
        let mut added_blocks = vec![];

        for block in damaged_blocks.iter() {
            if existing_blocks.contains(&block) {
                let name = format!("dynamic block {}", block);
                removed_blocks.push(name);
            }
            if self.block_types.contains_key(block) {
                let mut comp = Compilation::new(format!("dynamic block {}", block));
                let constraints = self.block_to_constraints.get(block).unwrap();

                for id in constraints.iter() {
                    if !damaged_constraints.contains(&id) { continue; }
                    if let Some(params) = self.constraint_to_params.get(&id) {
                        let block_type = self.block_type(*block, interner);
                        if let Some(compiled) = self.compile_constraint(params.clone(), interner, block_type) {
                            self.constraints.insert(*id, compiled);
                        }
                    }
                }

                comp.constraints.extend(constraints.iter().map(|&id| self.constraints.get(&id).unwrap()).cloned());
                comp.finalize();
                added_blocks.extend(compilation_to_blocks(comp, interner, "compiler_watcher", ""));
            }
        }

        if self.remote {
            let remote_adds = added_blocks.iter().map(|b| b.to_portable(interner)).collect();
            self.outgoing.send(RunLoopMessage::RemoteCodeTransaction(remote_adds, removed_blocks)).unwrap();
        } else {
            self.outgoing.send(RunLoopMessage::CodeTransaction(added_blocks, removed_blocks)).unwrap();
        }
    }
}
