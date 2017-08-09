use super::super::indexes::{WatchDiff};
use super::super::ops::{Interned, Internable, Interner, RawChange, RunLoopMessage, JSONInternable};
use std::sync::mpsc::{self, Sender};
use std::collections::hash_map::{Entry};
use std::collections::HashMap;
use super::Watcher;
use std::ops::{Neg, AddAssign, MulAssign};

extern crate futures;
extern crate hyper;
extern crate tokio_core;

extern crate serde_json;
extern crate serde;
use self::serde_json::{Map, Value, Error};

use std::io::{self, Write, BufReader};
use self::futures::{Future, Stream};
use self::hyper::Client;
use self::tokio_core::reactor::Core;

use watchers::json::{value_to_changes};

pub struct HttpWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
}

impl HttpWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> HttpWatcher {
        HttpWatcher { name: "http".to_string(), outgoing }
    }
}

impl Watcher for HttpWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {       
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let id = Internable::to_string(interner.get_value(add[1]));
            let address = Internable::to_string(interner.get_value(add[2]));
            let mut changes = vec![];
            match &kind[..] {
                "send" => {
                    send_http_request(address, id, &mut changes);
                    println!("{:?}", &changes);
                },
                _ => {},
            }           
            match self.outgoing.send(RunLoopMessage::Transaction(changes)) {
                Err(_) => break,
                _ => (),
            }
        }
    }
}

fn send_http_request(address: String, id: String, changes: &mut Vec<RawChange>) {
    let mut core = tokio_core::reactor::Core::new().unwrap();
    let handle = core.handle();
    let client = Client::new(&handle);
    let url = address.parse::<hyper::Uri>().unwrap();
    //let mut vec = Vec::new();
    let work = client.get(url).and_then(|res| {
        //println!("Response: {}", res.status());
        //println!("Headers: \n{}", res.headers());

        res.body().concat2().and_then(move |body| {
            let v: Value = serde_json::from_slice(&body).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    e
                )
            })?;
            value_to_changes(v, changes, &id, "http-response");
            Ok(())
        })
    });
    //println!("{:?}",vec);
    core.run(work).unwrap();
}