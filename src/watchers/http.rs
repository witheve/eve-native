use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use watchers::json::{new_change};
use super::Watcher;

extern crate data_encoding;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate tokio_core;
extern crate serde_json;
extern crate serde;
use self::futures::{Future, Stream};
use self::hyper::Client;
use self::hyper_tls::HttpsConnector;
use self::tokio_core::reactor::Core;
use self::hyper::{Method};
use std::thread;
use std::io::{Write};
extern crate iron;
use self::iron::prelude::*;
use self::iron::status;
use self::data_encoding::base64;
use std::collections::HashMap;

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
        let mut requests: HashMap<String,hyper::Request> = HashMap::new();
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let id = Internable::to_string(interner.get_value(add[1]));
            let address = Internable::to_string(interner.get_value(add[2]));   
            match &kind[..] {
                "request" => {
                    let body = Internable::to_string(interner.get_value(add[4]));
                    let key = Internable::to_string(interner.get_value(add[5]));
                    let value = Internable::to_string(interner.get_value(add[6])); 
                    if !requests.contains_key(&id) {
                        let url = address.parse::<hyper::Uri>().unwrap();
                        let method = Internable::to_string(interner.get_value(add[3]));
                        let rmethod: Method = match &method.to_lowercase()[..] {
                            "get"     => Method::Get,
                            "put"     => Method::Put,
                            "post"    => Method::Post,
                            "delete"  => Method::Delete,
                            "head"    => Method::Head,
                            "trace"   => Method::Trace,
                            "connect" => Method::Connect,
                            "patch"   => Method::Patch,
                            _         => Method::Get
                        };
                        let req = hyper::Request::new(rmethod, url);
                        requests.insert(id.clone(),req);
                    }
                    let req = requests.get_mut(&id).unwrap();
                    if key != "" {
                        req.headers_mut().set_raw(key, vec![value.into_bytes().to_vec()]);
                    }
                    if body != "" {
                        req.set_body(body);
                    }                    
                },
                "server" => {
                    let body = Internable::to_string(interner.get_value(add[3]));
                    http_server(address, body);
                },
                _ => {},
            }      
        }
        // Send the HTTP request and package response in the changevec
        for (id, request) in requests.drain() {
          send_http_request(&id,request,&self.outgoing);
        }
    }
}

fn http_server(address: String, body: String) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        Iron::new(|req: &mut Request| {
            Ok(Response::with((status::Ok, "Hello")))
        }).http(address).unwrap();
    })
}

fn send_http_request(id: &String, request: hyper::Request, outgoing: &Sender<RunLoopMessage>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let client = Client::configure()
        .connector(HttpsConnector::new(4,&handle).unwrap())
        .build(&handle);
    let work = client.request(request).and_then(|res| {
        let mut response_changes: Vec<RawChange> = vec![];
        let status = res.status().as_u16();
        let response_id = format!("http/response|{:?}",id);
        response_changes.push(new_change(&response_id, "tag", Internable::from_str("http/response"), "http/request"));
        response_changes.push(new_change(&response_id, "status", Internable::String(status.to_string()), "http/request"));
        response_changes.push(new_change(&response_id, "request", Internable::String(id.clone()), "http/request"));
        outgoing.send(RunLoopMessage::Transaction(response_changes)).unwrap();
        res.body().for_each(|chunk| {
            let response_id = format!("http/response|{:?}",id);
            let mut vector: Vec<u8> = Vec::new();
            vector.write_all(&chunk).unwrap();
            let body_string = String::from_utf8(vector).unwrap();
            outgoing.send(RunLoopMessage::Transaction(vec![new_change(&response_id, "body", Internable::String(body_string), "http/request")])).unwrap();
            Ok(())
        })
        
    });
    match core.run(work) {
        Ok(_) => (),
        Err(e) => {
            // Form an HTTP Error
            let error_id = format!("http/request/error|{:?}",&id);
            let mut error_changes: Vec<RawChange> = vec![];
            error_changes.push(new_change(&error_id, "tag", Internable::from_str("http/request/error"), "http/request"));
            error_changes.push(new_change(&error_id, "request", Internable::String(id.clone()), "http/request"));
            error_changes.push(new_change(&error_id, "error", Internable::String(format!("{:?}",e)), "http/request"));
            outgoing.send(RunLoopMessage::Transaction(error_changes)).unwrap();
        },
    }
}