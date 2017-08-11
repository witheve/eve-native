use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use watchers::json::{value_to_changes, new_change};
use super::Watcher;

extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate tokio_core;
extern crate serde_json;
extern crate serde;
use self::serde_json::{Value};
use self::futures::{Future, Stream};
use self::hyper::Client;
use self::hyper_tls::HttpsConnector;
use self::tokio_core::reactor::Core;
use self::hyper::header::ContentLength;
use self::hyper::{Method, Request};
use self::hyper::server::{Http, Response, Service};
use std::thread;
use std::io::{self, Write};
use std::fs::File;

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
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let client = Client::configure()
        .connector(HttpsConnector::new(4,&handle).unwrap())
        .build(&handle);
    let url = address.parse::<hyper::Uri>().unwrap();
    println!("{:?}",url);
    //let mut vec = Vec::new();
    let req = Request::new(Method::Get, url);

    /*
    let work = client.request(req)
                     
                     .and_then(|res| {
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
    });*/

    
    let work = client.request(req).and_then(|res| {
        let status = res.status().as_u16();
        let response_id = format!("http/response|{:?}",id);
        changes.push(new_change(&response_id, "tag", Internable::from_str("http/response"), "http/request"));
        changes.push(new_change(&response_id, "status", Internable::String(status.to_string()), "http/request"));
        changes.push(new_change(&response_id, "request", Internable::String(id.clone()), "http/request"));
        println!("Response: {}", res.status());
        res.body().for_each(|chunk| {
            let response_id = format!("http/response|{:?}",id);
            let mut vector: Vec<u8> = Vec::new();
            vector.write_all(&chunk);
            let body_string = String::from_utf8(vector).unwrap();
            changes.push(new_change(&response_id, "body", Internable::String(body_string), "http/request"));
            Ok(())
        })
    });



    match core.run(work) {
        Ok(_) => println!("OK"),
        Err(e) => {
            // Form an HTTP Error
            //let error_id = format!("http/request/error|{:?}|{:?}",&id,address);
            //changes.push(new_change(&error_id, "tag", Internable::from_str("http/request/error"), "http/request"));
            //changes.push(new_change(&error_id, "request", Internable::String(id), "http/request"));
            //changes.push(new_change(&error_id, "error", Internable::String(format!("{:?}",e)), "http/request"));
            println!("Not OK {:?}",e)
        },
    }
}