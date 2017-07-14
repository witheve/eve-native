#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate tokio_timer;
extern crate futures;

extern crate ws;

use ws::{listen, Message, Sender, Handler, CloseCode};
use std::sync::mpsc::{SyncSender};

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_json;

extern crate serde;

use serde_json::{Error};

extern crate eve;
extern crate time;

use eve::ops::{Program, Transaction, RawChange, Internable, Interner};
use eve::indexes::{WatchDiff};
use eve::parser::{parse_file};
use eve::watcher::{SystemTimerWatcher, Watcher};
use std::env;
use std::thread;


lazy_static! {
    static ref CLOSE:RawChange = RawChange {e:Internable::Null, a:Internable::Null, v:Internable::Null, n:Internable::Null, count:0};
}

pub struct ClientHandler {
    out: Sender,
    program_input: SyncSender<Vec<RawChange>>,
}

impl ClientHandler {
    pub fn new(out:Sender) -> ClientHandler {
        let program_input = make_program(out.clone());
        ClientHandler {out, program_input}
    }
}

impl Handler for ClientHandler {
    fn on_message(&mut self, msg: Message) -> Result<(), ws::Error> {
        println!("Server got message '{}'. ", msg);
        if let Message::Text(s) = msg {
            let deserialized: Result<ClientMessage, Error> = serde_json::from_str(&s);
            println!("deserialized = {:?}", deserialized);
            match deserialized {
                Ok(ClientMessage::Transaction { adds, removes }) => {
                    let mut raw_changes = vec![];
                    raw_changes.extend(adds.into_iter().map(|(e,a,v)| {
                        RawChange { e,a,v,n:Internable::String("input".to_string()),count:1 }
                    }));
                    raw_changes.extend(removes.into_iter().map(|(e,a,v)| {
                        RawChange { e,a,v,n: Internable::String("input".to_string()),count:-1 }
                    }));
                    self.program_input.send(raw_changes).unwrap();
                    println!("Got transaction!");
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
        self.program_input.send(vec![CLOSE.clone()]).unwrap();
    }
}

fn make_program(out:Sender) -> SyncSender<Vec<RawChange>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let local_close = CLOSE.clone();
        let mut program = Program::new();
        sender.send(program.outgoing.clone()).unwrap();
        let outgoing = program.outgoing.clone();
        program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
        program.attach("client/websocket", Box::new(WebsocketClientWatcher::new(out)));

        for file in env::args().skip(1) {
            let blocks = parse_file(&mut program, &file);
            for block in blocks {
                program.raw_block(block);
            }
        }
        println!("Starting run loop.");
        'outer: loop {
            match program.incoming.recv() {
                Ok(v) => {
                    let start_ns = time::precise_time_ns();
                    let mut txn = Transaction::new();
                    for cur in v {
                        if cur == local_close { break 'outer; }
                        txn.input_change(cur.to_change(&mut program.state.interner));
                    };
                    txn.exec(&mut program);
                    let end_ns = time::precise_time_ns();
                    println!("Txn took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
                }
                Err(_) => { break; }
            }
        }
        println!("Closing run loop.");
    });
    receiver.recv().unwrap()
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

