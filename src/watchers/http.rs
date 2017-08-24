use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use watchers::json::{new_change};
use super::Watcher;

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
use std::io;
use std::io::{Write};
extern crate iron;
use self::iron::prelude::*;
use self::iron::status;
use std::collections::HashMap;

pub struct HttpWatcher {
    name: String,
    responses: HashMap<String,Vec<(u32,String)>>,
    outgoing: Sender<RunLoopMessage>,
}

impl HttpWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> HttpWatcher {
        HttpWatcher { name: "http".to_string(), responses: HashMap::new(), outgoing }
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
                "body" => {
                    let response_id = Internable::to_string(interner.get_value(add[1]));
                    let chunk = Internable::to_string(interner.get_value(add[2]));
                    let index = Internable::to_number(interner.get_value(add[3])) as u32;
                    if self.responses.contains_key(&response_id) {
                        match self.responses.get_mut(&response_id) {
                            Some(v) => v.push((index,chunk)),
                            _ => (),
                        }
                    } else {
                        self.responses.insert(response_id,vec![(index.clone(),chunk.clone())]);
                    }
                }
                _ => {},
            }      
        }
        // Send the HTTP request
        for (id, request) in requests.drain() {
          send_http_request(&id,request,&self.outgoing);
        };
        
        for (response_id, mut chunk_vec) in self.responses.drain() {
            chunk_vec.sort();
            let body: String = chunk_vec.iter().fold("".to_string(), |acc, ref x| {
                let &&(ref ix, ref chunk) = x;
                acc + chunk
            });
            let response_id = format!("http/full-body|{:?}",response_id);
            self.outgoing.send(RunLoopMessage::Transaction(vec![
                new_change(&response_id, "tag", Internable::from_str("http/fully-body"), "http/request"),
                new_change(&response_id, "body", Internable::String(body), "http/request"),
            ])).unwrap();
        };
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
    let node = "http/request";
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let client = Client::configure()
        .connector(HttpsConnector::new(4,&handle).unwrap())
        .build(&handle);
    let mut ix: f32 = 1.0;
    let work = client.request(request).and_then(|res| {
        let mut response_changes: Vec<RawChange> = vec![];
        let status = res.status().as_u16();
        let response_id = format!("http/response|{:?}",id);
        response_changes.push(new_change(&response_id, "tag", Internable::from_str("http/response"), node));
        response_changes.push(new_change(&response_id, "status", Internable::String(status.to_string()), node));
        response_changes.push(new_change(&response_id, "request", Internable::String(id.clone()), node));
        outgoing.send(RunLoopMessage::Transaction(response_changes)).unwrap();
        res.body().for_each(|chunk| {
            let response_id = format!("http/response|{:?}",id);
            let chunk_id = format!("body-chunk|{:?}|{:?}",&response_id,ix);
            let mut vector: Vec<u8> = Vec::new();
            vector.write_all(&chunk).unwrap();
            let body_string = String::from_utf8(vector).unwrap();
            outgoing.send(RunLoopMessage::Transaction(vec![
                new_change(&chunk_id, "tag", Internable::from_str("http/body-chunk"), node),
                new_change(&chunk_id, "request", Internable::from_str(id), node),
                new_change(&chunk_id, "chunk", Internable::String(body_string), node),
                new_change(&chunk_id, "index", Internable::from_number(ix), node)
            ])).unwrap();
            ix = ix + 1.0;
            Ok(())
        })
    });
    match core.run(work) {
        Ok(_) => (),
        Err(e) => {
            // Form an HTTP Error
            let error_id = format!("http/request/error|{:?}",&id);
            let mut error_changes: Vec<RawChange> = vec![];
            error_changes.push(new_change(&error_id, "tag", Internable::from_str("http/request/error"), node));
            error_changes.push(new_change(&error_id, "request", Internable::String(id.clone()), node));
            error_changes.push(new_change(&error_id, "error", Internable::String(format!("{:?}",e)), node));
            outgoing.send(RunLoopMessage::Transaction(error_changes)).unwrap();
        },
    }
    let error_id = format!("http/request/error|123456");
    let mut changes: Vec<RawChange> = vec![];
    changes.push(new_change(&error_id, "tag", Internable::from_str("http/request/done"), node));
    changes.push(new_change(&error_id, "request", Internable::from_str(id), node));
    outgoing.send(RunLoopMessage::Transaction(changes)).unwrap();
}