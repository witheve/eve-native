use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use super::Watcher;

extern crate serde_json;
extern crate serde;
use self::serde_json::{Map, Value, Number};
use std::collections::HashMap;

pub struct JsonWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
    join_strings_map: HashMap<String, JoinStrings>,
}

impl JsonWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> JsonWatcher {
        JsonWatcher { name: "json".to_string(), join_strings_map: HashMap::new(), outgoing }
    }
}

#[derive(Debug, Clone)]
pub struct JoinStrings {
    strings: Vec<String>,
    with: String
}

impl JoinStrings {
    pub fn new(with: String) -> JoinStrings {
        JoinStrings { with, strings: vec![]  }
    }
    pub fn join(&self) -> String {
        self.strings.join(self.with.as_ref())
    }
}

impl Watcher for JsonWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        let mut changes: Vec<RawChange> = vec![];
        for remove in diff.removes {
            let kind = Internable::to_string(interner.get_value(remove[0]));
            match kind.as_ref() {
                "join" => {
                    let id = Internable::to_string(interner.get_value(remove[1]));
                    let string = Internable::to_string(interner.get_value(remove[2]));
                    let with = Internable::to_string(interner.get_value(remove[3]));
                    let join_strings = self.join_strings_map.get_mut(&id).unwrap();
                    let index = join_strings.strings.iter().position(|x| *x == string).unwrap();
                    join_strings.strings.remove(index);
                },
                _ => {},
            }
        }
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let record_id = Internable::to_string(interner.get_value(add[1]));
            match kind.as_ref() {
                "decode" => {
                    let value = Internable::to_string(interner.get_value(add[2]));
                    let v: Value = serde_json::from_str(&value).unwrap();
                    let change_id = format!("json/decode/change|{:?}",record_id);
                    value_to_changes(change_id.as_ref(), "json-object", v, "json/decode", &mut changes);
                    changes.push(new_change(&change_id, "tag", Internable::from_str("json/decode/change"), "json/decode"));
                    changes.push(new_change(&change_id, "decode", Internable::String(record_id), "json/decode"));
                },
                "join" => {
                    let id = Internable::to_string(interner.get_value(add[1]));
                    let string = Internable::to_string(interner.get_value(add[2]));
                    let with = Internable::to_string(interner.get_value(add[3]));
                    if self.join_strings_map.contains_key(&id) {
                        let join_strings = self.join_strings_map.get_mut(&id).unwrap();
                        join_strings.strings.push(string);
                    } else {
                        let mut join_strings = JoinStrings::new(with);
                        join_strings.strings.push(string);
                        self.join_strings_map.insert(id, join_strings);
                    }
                },
                _ => {},
            }
        }

        for (record_id, join_strings) in self.join_strings_map.iter() {
            let join_id = format!("string/join|{:?}",record_id);
            changes.push(new_change(&join_id, "tag", Internable::from_str("string/join/result"), "string/join"));
            changes.push(new_change(&join_id, "result", Internable::String(join_strings.join()), "string/join"));
            changes.push(new_change(&join_id, "record", Internable::String(record_id.to_owned()), "string/join"));
        }
        match self.outgoing.send(RunLoopMessage::Transaction(changes)) {
            Err(_) => (),
            _ => (),
        }   
    }
}

pub fn new_change(e: &str, a: &str, v: Internable, n: &str) -> RawChange {
    RawChange {e: Internable::from_str(e), a: Internable::from_str(a), v: v.clone(), n: Internable::from_str(n), count: 1}
}

pub fn value_to_changes(id: &str, attribute: &str, value: Value, node: &str, changes: &mut Vec<RawChange>) {
    match value {
        Value::Number(n) => {    
            if n.is_u64() { 
                let v = Internable::from_number(n.as_u64().unwrap() as f32); 
                changes.push(new_change(id,attribute,v,node));
            } else if n.is_i64() {
                let v = Internable::from_number(n.as_i64().unwrap() as f32); 
                changes.push(new_change(id,attribute,v,node));
            } else if n.is_f64() { 
                let v = Internable::from_number(n.as_f64().unwrap() as f32); 
                changes.push(new_change(id,attribute,v,node));
            };
        },
        Value::String(ref n) => {
            changes.push(new_change(id,attribute,Internable::String(n.clone()),node));
        },
        Value::Bool(ref n) => {
            let b = match n {
                &true => "true",
                &false => "false",
            };
            changes.push(new_change(id,attribute,Internable::from_str(b),node));
        },
        Value::Array(ref n) => {
            for (ix, value) in n.iter().enumerate() {
                let ix = ix + 1;
                let array_id = format!("array|{:?}|{:?}|{:?}", id, ix, value);
                let array_id = &array_id[..];
                changes.push(new_change(id,attribute,Internable::from_str(array_id),node));
                changes.push(new_change(array_id,"tag",Internable::from_str("array"),node));
                changes.push(new_change(array_id,"index",Internable::String(ix.to_string()),node));
                value_to_changes(array_id, "value", value.clone(), node, changes);
            }
        },
        Value::Object(ref n) => {
            let object_id = format!("{:?}",n);
            changes.push(new_change(id,attribute,Internable::String(object_id.clone()),node));
            changes.push(new_change(id,"tag",Internable::from_str("json-object"),node));
            for key in n.keys() {
                value_to_changes(&mut object_id.clone(), key, n[key].clone(), node, changes);
            }
        },
    _ => {},
    }  
}