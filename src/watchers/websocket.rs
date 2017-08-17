extern crate serde_json;


extern crate ws;
use self::ws::{Sender, Message};
use super::super::indexes::{WatchDiff};
use super::super::ops::{Interner, JSONInternable};

use super::{Watcher};

//-------------------------------------------------------------------------
// Websocket client watcher
//-------------------------------------------------------------------------

pub struct WebsocketClientWatcher {
    name: String,
    outgoing: Sender,
    client_name: String,
}

impl WebsocketClientWatcher {
    pub fn new(outgoing: Sender, client_name: &str) -> WebsocketClientWatcher {
        WebsocketClientWatcher { name: "client/websocket".to_string(), outgoing, client_name: client_name.to_owned() }
    }
}

impl Watcher for WebsocketClientWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        let adds:Vec<Vec<JSONInternable>> = diff.adds.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v).into()).collect()
        }).collect();
        let removes:Vec<Vec<JSONInternable>> = diff.removes.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v).into()).collect()
        }).collect();
        let text = serde_json::to_string(&json!({"adds": adds, "removes": removes, "client": self.client_name})).unwrap();
        self.outgoing.send(Message::Text(text)).unwrap();
    }
}
