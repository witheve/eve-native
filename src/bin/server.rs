#![feature(link_args)]
#![feature(dropck_eyepatch)]
#![feature(generic_param_attrs)]
#![feature(sip_hash_13)]
#![feature(core_intrinsics)]
#![feature(shared)]
#![feature(unique)]
#![feature(placement_new_protocol)]
#![feature(fused)]
#![feature(alloc)]
#![feature(heap_api)]
#![feature(oom)]
#![feature(slice_patterns)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

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

