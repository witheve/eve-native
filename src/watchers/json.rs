use super::super::indexes::{WatchDiff};
use super::super::ops::{Interned, Internable, Interner, RawChange, RunLoopMessage, JSONInternable};
use std::sync::mpsc::{self, Sender};
use std::collections::hash_map::{Entry};
use std::collections::HashMap;
use super::Watcher;
use std::ops::{Neg, AddAssign, MulAssign};
use serde::de::{self, Deserialize, Deserializer, DeserializeSeed, Visitor, SeqAccess,
                MapAccess, EnumAccess, VariantAccess, IntoDeserializer};
use serde::ser::{Serialize, Serializer};

extern crate serde_json;
extern crate serde;
use self::serde_json::{Map, Value};
use std::fmt;
use std::error::Error;
use std::mem::transmute;
use std::marker::PhantomData;

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
        println!("Making changes");
        let mut record_map = Map::new();
        let mut id = "".to_string();
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let record_id = Internable::to_string(interner.get_value(add[1]));
            let j_arg = Internable::to_string(interner.get_value(add[2]));
            id = j_arg.clone();
            let mut changes: Vec<RawChange> = vec![];
            match (&kind[..], j_arg) {
                ("encode", j_arg) => {
                    let e = j_arg;
                    let a = Internable::to_string(interner.get_value(add[3]));
                    let v = Internable::to_string(interner.get_value(add[4]));
                    println!("[e: {:?} a: {:?} v: {:?}]",e,a,v);
                    if record_map.contains_key(&e) {
                        let mut record = record_map.get_mut(&e).unwrap();
                        let r_map = record.as_object_mut().unwrap();
                        r_map.insert(a, Value::String(v));
                    } else {
                        let mut new_record = Map::new();
                        new_record.insert(a, Value::String(v));
                        record_map.insert(e, Value::Object(new_record));
                    }
                },
                ("decode", j_arg) => {
                    let v: Value = serde_json::from_str(&j_arg).unwrap();
                    value_to_changes(&mut record_id.to_string(), "json-object", v, &mut changes);
                },
                _ => {},
            }           
            match self.outgoing.send(RunLoopMessage::Transaction(changes)) {
                Err(_) => break,
                _ => (),
            }
        }
        //let json = serde_json::to_string(&record_map).unwrap();
        
        //let target_record = record_map.get_mut(&id);

        //println!("{:?}",target_record);
        //chchanges.push(RawChange {e: Internable::String(id.clone().to_string()), a: Internable::String("json-string".to_string()), v: Internable::String(json.clone()), n: Internable::String("json/encode".to_string()), count: 1});
        //match self.outgoing.send(RunLoopMessage::Transaction(chchanges)) {
        //    Err(_) => (),
        //    _ => (),
        // }
    }
}

fn newChange(e: &str, a: &str, v: Internable, n: &str) -> RawChange {
    RawChange {e: Internable::String(e.to_string()), a: Internable::String(a.to_string()), v: v.clone(), n: Internable::String(n.to_string()), count: 1}
}

fn value_to_changes(id: &str, attribute: &str, value: Value, changes: &mut Vec<RawChange>) {
    let node = "json/decode";
    match value {
        Value::Number(n) => {    
            if n.is_u64() { 
                let v = Internable::Number(n.as_u64().unwrap() as u32); 
                changes.push(newChange(id,attribute,v,node));
            } else if n.is_i64() {
                let v = Internable::Number(n.as_i64().unwrap() as u32); 
                changes.push(newChange(id,attribute,v,node));
            } else if n.is_f64() { 
                let v = Internable::from_number(n.as_f64().unwrap() as f32); 
                changes.push(newChange(id,attribute,v,node));
            };
        },
        Value::String(ref n) => {
            changes.push(newChange(id,attribute,Internable::String(n.clone()),node));
        },
        Value::Bool(ref n) => println!("Bool: {}",n),
        Value::Array(ref n) => {
            for (ix, value) in n.iter().enumerate() {
                let ix = ix + 1;
                let array_id = format!("array|{:?}|{:?}",ix,value);
                let array_id = &array_id[..];
                changes.push(newChange(id,attribute,Internable::String(array_id.to_string()),node));
                changes.push(newChange(array_id,"tag",Internable::String("array".to_string()),node));
                changes.push(newChange(array_id,"index",Internable::String(ix.to_string()),node));
                value_to_changes(array_id, "value", value.clone(), changes);
            }
        },
        Value::Object(ref n) => {
            let idq = format!("{:?}",n);
            changes.push(newChange(id,attribute,Internable::String(idq.clone()),node));
            changes.push(newChange(id,"tag",Internable::String("json-object".to_string()),node));
            for key in n.keys() {
                value_to_changes(&mut idq.clone(), key, n[key].clone(), changes);
            }
        },
    _ => {},
    }  
}   

/*
#[derive(Debug)]
pub enum ChangeVec {
    Changes(Vec<RawChange>)
}

impl ChangeVec {
    pub fn new() -> ChangeVec {
        ChangeVec::Changes(Vec::new())
    }
}

impl<'de> Deserialize<'de> for ChangeVec {

    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct ChangeVecVisitor {
            marker: PhantomData<ChangeVec>
        }

        impl ChangeVecVisitor {
            fn new() -> Self {
                ChangeVecVisitor {
                    marker: PhantomData
                }
            }
        }

        impl<'de> Visitor<'de> for ChangeVecVisitor {
            type Value = ChangeVec;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expecting a thing")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
                where M: MapAccess<'de>
            {
                let mut vec = Vec::new();
                while let Some(kv) = try!(access.next_entry()) {
                    vec.push(kv);
                }
                Ok(ChangeVec::new())
            }
        }

        deserializer.deserialize_any(ChangeVecVisitor::new())
    }
}*/