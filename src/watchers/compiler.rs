use super::super::ops::{make_scan, make_function, Constraint, Interned, Internable, Interner, Field, RunLoopMessage};
use super::super::compiler::{get_function_info};
use indexes::{WatchDiff};
use std::sync::mpsc::{Sender};
use super::Watcher;
use compiler::{Compilation, compilation_to_blocks};
use std::collections::{HashMap, HashSet, BTreeMap};
use std::collections::hash_map::{Entry};

pub enum ConstraintParams {
    Scan(Interned, Interned, Interned),
    Output(Interned, Interned, Interned),
    Function(Interned, Interned, HashMap<String, Interned>),
    Variadic(Interned, Interned, Vec<Interned>),
    GenId(Interned, BTreeMap<Interned, Interned>)
}

//-------------------------------------------------------------------------
// Compiler Watcher
//-------------------------------------------------------------------------

pub struct CompilerWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
    variable_ix: usize,
    variables: HashMap<Interned, Field>,
    block_types: HashMap<Interned, Interned>,
    block_to_constraints: HashMap<Interned, Vec<Interned>>,
    constraint_to_params: HashMap<Interned, ConstraintParams>,
    constraints: HashMap<Interned, Constraint>,
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> CompilerWatcher {
        CompilerWatcher{name: "eve/compiler".to_string(),
                        outgoing,
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

    pub fn get_field(&self, value:Interned) -> Field {
        self.variables.get(&value).cloned().unwrap_or_else(|| Field::Value(value))
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

    pub fn args_to_vec(&self, op:&str, args:&HashMap<String, Interned>) -> Option<Vec<Field>> {
        if let Some(info) = get_function_info(op) {
            let mut params:Vec<Field> = vec![];
            for param in info.get_params() {
                if let Some(&val) = args.get(param) {
                    if val != 0 {
                        params.push(self.get_field(val));
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            return Some(params);
        }
        None
    }

    pub fn varargs_to_vec(&self, args:&Vec<Interned>) -> Option<Vec<Field>> {
        Some(args.iter().map(|&arg| self.get_field(arg)).collect())
    }

    pub fn identity_to_vec(&self, args:&BTreeMap<Interned, Interned>) -> Option<Vec<Field>> {
        // @FIXME: needs attr to soon.
        Some(args.iter().map(|(&attr, &val)| self.get_field(val)).collect())
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
                    ("gen-id", &[id, block, ..])=> {
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
                        self.constraint_to_params.insert(id, ConstraintParams::GenId(variable, BTreeMap::new()));
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
                        match params {
                            &ConstraintParams::Scan(e, a, v) => {
                                let scan = make_scan(self.get_field(e), self.get_field(a), self.get_field(v));
                                self.constraints.insert(*id, scan);
                            },
                            &ConstraintParams::Output(e, a, v) => {
                                let is_commit = self.block_type(*block, interner).unwrap() == "commit";
                                let output = Constraint::Insert{e: self.get_field(e), a: self.get_field(a), v: self.get_field(v), commit: is_commit};
                                self.constraints.insert(*id, output);
                            },
                            &ConstraintParams::Function(op_interned, output, ref params_interned) => {
                                if let &Internable::String(ref op) = interner.get_value(op_interned) {
                                    if let Some(params) = self.args_to_vec(op, &params_interned) {
                                        let function = make_function(op, params, self.get_field(output));
                                        self.constraints.insert(*id, function);
                                    }
                                }
                            },
                            &ConstraintParams::Variadic(op_interned, output, ref params_interned) => {
                                if let &Internable::String(ref op) = interner.get_value(op_interned) {
                                    if let Some(params) = self.varargs_to_vec(&params_interned) {
                                        let function = make_function(op, params, self.get_field(output));
                                        self.constraints.insert(*id, function);
                                    }
                                }
                            },
                            &ConstraintParams::GenId(var, ref identity_attrs) => {
                                if let Some(params) = self.identity_to_vec(identity_attrs) {
                                    println!("GEN ID VAR: {:?} ARGS {:?}", self.get_field(var), params);
                                    let function = make_function("gen_id", params, self.get_field(var));
                                    self.constraints.insert(*id, function);
                                }
                            }
                        }
                    }
                }

                comp.constraints.extend(constraints.iter().map(|&id| self.constraints.get(&id).unwrap()).cloned());
                comp.finalize();
                added_blocks.extend(compilation_to_blocks(comp, "compiler_watcher", ""));
            }
        }

        // @FIXME: Gotta plumb remove all the way through.
        self.outgoing.send(RunLoopMessage::CodeTransaction(added_blocks, removed_blocks)).unwrap();
    }
}
