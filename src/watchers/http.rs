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

use std::io::{self, Write};
use futures::{Future, Stream};
use self::hyper::Client;
use self::tokio_core::reactor::Core;

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
            let address = Internable::to_string(interner.get_value(add[2]));
            
            //let record_id = Internable::to_string(interner.get_value(add[1]));
            //let j_arg = Internable::to_string(interner.get_value(add[2]));
            //id = j_arg.clone();
            let mut changes = vec![];
            match &kind[..] {
                "send" => {

                  send_http_request(address);



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


fn send_http_request(address: String) {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let handle = core.handle();
        let client = Client::new(&handle);
        let url = address.parse::<hyper::Uri>().unwrap();
        let work = client.get(url).and_then(|res| {
            println!("Response: {}", res.status());
            println!("Headers: \n{}", res.headers());
            res.body().for_each(|chunk| {
                io::stdout().write_all(&chunk).map_err(From::from)
            })
        }).map(|_| {
            println!("\n\nDone.");
        });
        core.run(work).unwrap();
}