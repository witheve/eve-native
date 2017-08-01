use super::super::indexes::{WatchDiff};
use super::super::ops::{Interned, Internable, Interner, RawChange, RunLoopMessage, JSONInternable};
use std::sync::mpsc::{self, Sender};
use std::collections::hash_map::{Entry};
use super::Watcher;

extern crate serde_json;
extern crate serde;
use self::serde_json::{Map, Value, Error};

pub struct JsonWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
}

impl JsonWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> JsonWatcher {
        JsonWatcher { name: "json".to_string(), outgoing }
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
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let record_id = Internable::to_string(interner.get_value(add[1]));
            let j_arg = Internable::to_string(interner.get_value(add[2]));
            let mut changes = vec![];
            match (&kind[..], j_arg) {
                ("decode", j_arg) => {
                    let v: Value = serde_json::from_str(&j_arg).unwrap();
                    value_to_changes(v, &mut changes, &mut record_id.to_string(), "json-object");
                },
                ("enocde", j_arg) => {
                    println!("encoding:\n{:?}",j_arg);
                }
                _ => {},
            }           
            match self.outgoing.send(RunLoopMessage::Transaction(changes)) {
                Err(_) => break,
                _ => (),
            }
        }
    }
}

fn value_to_changes(value: Value, changes: &mut Vec<RawChange>, id: &mut String, attribute: &str) {
    match value {
        Value::Number(n) => {
            if n.is_u64() { 
                let v = Internable::Number(n.as_u64().unwrap() as u32); 
                changes.push(RawChange {e: Internable::String(id.clone()), a: Internable::String(attribute.to_string()), v, n: Internable::String("json/decode".to_string()), count: 1});
            } else if n.is_f64() { 
                let v = Internable::from_number(n.as_f64().unwrap() as f32); 
                changes.push(RawChange {e: Internable::String(id.clone()), a: Internable::String(attribute.to_string()), v, n: Internable::String("json/decode".to_string()), count: 1});
            };
        },
        Value::String(ref n) => {
            changes.push(RawChange {e: Internable::String(id.clone()), a: Internable::String(attribute.to_string()), v: Internable::String(n.clone()), n: Internable::String("json/decode".to_string()), count: 1});
        },
        Value::Bool(ref n) => println!("Bool: {}",n),
        Value::Array(ref n) => {
            for v in n {
                value_to_changes(v.clone(), changes, id, attribute);
            }
        },
        Value::Object(ref n) => {
            let idq = format!("{:?}",n);
            changes.push(RawChange {e: Internable::String(id.clone()), a: Internable::String(attribute.to_string()), v: Internable::String(idq.clone()), n: Internable::String("json/decode".to_string()), count: 1});
            changes.push(RawChange {e: Internable::String(id.clone()), a: Internable::String("tag".to_string()), v: Internable::String("json-object".to_string()), n: Internable::String("json/decode".to_string()), count: 1});
            for key in n.keys() {
                value_to_changes(n[key].clone(), changes, &mut idq.clone(), key);
            }
        },
    _ => {},
    }  
}   