use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use std::io::{self};
use watchers::json::{value_to_changes, new_change};
use super::Watcher;

extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate serde_json;
extern crate serde;
use self::serde_json::{Value};
use self::futures::{Future, Stream};
use self::hyper::Client;
//use self::hyper::header::ContentLength;
//use self::hyper::server::{Http, Request, Response, Service};
//use std::thread;

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
                "request" => {
                    send_http_request(address, id, &mut changes);
                },
                "server" => {
                    println!("Starting HTTP Server");
                    //http_server(address);
                    println!("HTTP Server started");
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

/*fn http_server(address: String, service: str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        println!("Starting HTTP Server at {}... ", address);
        let addr = address.parse().unwrap();
        let server = Http::new().bind(&addr, || Ok(service)).unwrap();
        server.run().unwrap();
    })
}*/

fn send_http_request(address: String, id: String, changes: &mut Vec<RawChange>) {
    let mut core = tokio_core::reactor::Core::new().unwrap();
    let handle = core.handle();
    let client = Client::new(&handle);
    let url = address.parse::<hyper::Uri>().unwrap();
    //let mut vec = Vec::new();
    let work = client.get(url).and_then(|res| {
        let status = res.status().as_u16();
        // TODO Ship headers back to Eve
        //println!("Headers: \n{:?}", res.headers());
        res.body().concat2().and_then(move |body| {
            let v: Value = serde_json::from_slice(&body).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    e
                )
            })?;
            let response_id = format!("http/response|{:?}",id);
            changes.push(new_change(&response_id, "tag", Internable::from_str("http/response"), "http/request"));
            changes.push(new_change(&response_id, "request", Internable::String(id), "http/request"));
            changes.push(new_change(&response_id, "status", Internable::String(status.to_string()), "http/request"));
            value_to_changes(&response_id, "body", v, "http/request", changes);
            Ok(())
        })
    });
    core.run(work).unwrap();
}