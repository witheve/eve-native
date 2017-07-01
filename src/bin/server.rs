#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate tokio_timer;
extern crate futures;

extern crate ws;

use ws::{listen, Message};

#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

use serde_json::{Error};

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Block { id:String, code:String },
    RemoveBlock { id:String },
    Yo { message:String },
}

fn main() {
  listen("127.0.0.1:3012", |out| {
      move |msg| {
          if let Message::Text(s) = msg {
              let deserialized: Result<ClientMessage, Error> = serde_json::from_str(&s);
              println!("deserialized = {:?}", deserialized);
              out.send(Message::text(serde_json::to_string(&ClientMessage::Yo {message: format!("{} - yo", s)}).unwrap()))
          } else {
                Ok(())
          }
      }
  }).unwrap()
}

