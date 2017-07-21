#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate tokio_timer;
extern crate futures;

extern crate ws;

use ws::{listen, Message, Sender, Handler, CloseCode};

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate serde;

use serde_json::{Error};

extern crate eve;
extern crate time;

use eve::ops::{ProgramRunner, RunLoop, RawChange, Internable, Interner};
use eve::indexes::{WatchDiff};
use eve::watcher::{SystemTimerWatcher, Watcher};
use std::env;

pub struct ClientHandler {
    out: Sender,
    running: RunLoop,
}

impl ClientHandler {
    pub fn new(out:Sender) -> ClientHandler {
        let mut runner = ProgramRunner::new();
        let outgoing = runner.program.outgoing.clone();
        runner.program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
        runner.program.attach("client/websocket", Box::new(WebsocketClientWatcher::new(out.clone())));

        for file in env::args().skip(1) {
            runner.load(&file);
        }

        let running = runner.run();
        ClientHandler {out, running}
    }
}

impl Handler for ClientHandler {
    fn on_message(&mut self, msg: Message) -> Result<(), ws::Error> {
        // println!("Server got message '{}'. ", msg);
        if let Message::Text(s) = msg {
            let deserialized: Result<ClientMessage, Error> = serde_json::from_str(&s);
            // println!("deserialized = {:?}", deserialized);
            match deserialized {
                Ok(ClientMessage::Transaction { adds, removes }) => {
                    println!("Got transaction!");
                    let mut raw_changes = vec![];
                    raw_changes.extend(adds.into_iter().map(|(e,a,v)| {
                        RawChange { e,a,v,n:Internable::String("input".to_string()),count:1 }
                    }));
                    raw_changes.extend(removes.into_iter().map(|(e,a,v)| {
                        RawChange { e,a,v,n: Internable::String("input".to_string()),count:-1 }
                    }));
                    self.running.send(raw_changes);
                }
                _ => { }
            }
            self.out.send(Message::text(serde_json::to_string(&ClientMessage::Yo {message: format!("{} - yo", s)}).unwrap()))
        } else {
            Ok(())
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.running.close();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Block { id:String, code:String },
    RemoveBlock { id:String },
    Transaction { adds: Vec<(Internable, Internable, Internable)>, removes: Vec<(Internable, Internable, Internable)> },
    Yo { message:String },
}

fn main() {
  listen("127.0.0.1:3012", |out| {
      ClientHandler::new(out)
  }).unwrap()
}


pub struct WebsocketClientWatcher {
    outgoing: Sender,
}

impl WebsocketClientWatcher {
    pub fn new(outgoing: Sender) -> WebsocketClientWatcher {
        WebsocketClientWatcher { outgoing }
    }
}

impl Watcher for WebsocketClientWatcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff) {
        let adds:Vec<Vec<&Internable>> = diff.adds.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v)).collect()
        }).collect();
        let removes:Vec<Vec<&Internable>> = diff.removes.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v)).collect()
        }).collect();
        let text = serde_json::to_string(&json!({"adds": adds, "removes": removes})).unwrap();
        self.outgoing.send(Message::Text(text)).unwrap();
    }
}

