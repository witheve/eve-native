use super::super::indexes::{WatchDiff, RawRemoteChange};
use super::super::ops::{Internable, Interner, Interned, RunLoopMessage, RawChange};
use super::Watcher;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;

//-------------------------------------------------------------------------
// Util
//-------------------------------------------------------------------------

fn s(string: &str) -> Internable {
   Internable::String(string.to_string())
}

fn n(num: f32) -> Internable {
   Internable::from_number(num)
}

//-------------------------------------------------------------------------
// Router
//-------------------------------------------------------------------------

pub enum RouterMessage {
    Remote(Vec<RawRemoteChange>),
    Local(String, Vec<RawChange>)
}

pub struct Router {
    manager: Sender<RunLoopMessage>,
    outgoing: Sender<RouterMessage>,
    clients: Arc<Mutex<HashMap<String, Sender<RunLoopMessage>>>>
}

impl Router {
    pub fn new(manager: Sender<RunLoopMessage>) -> Router {
        let (outgoing, incoming) = mpsc::channel();
        let clients = Arc::new(Mutex::new(HashMap::new()));
        let clients2:Arc<Mutex<HashMap<String, Sender<RunLoopMessage>>>> = clients.clone();
        thread::spawn(move || {
            let mut grouping:HashMap<Internable, Vec<RawRemoteChange>> = HashMap::new();
            loop {
                match incoming.recv().unwrap() {
                    RouterMessage::Remote(remotes) => {
                        for remote in remotes {
                            // @FIXME is there really no way to do this without always cloning the to? :(
                            let vs = grouping.entry(remote.to.clone()).or_insert_with(|| vec![]);
                            vs.push(remote);
                        }
                        for (key, changes) in grouping.drain() {
                            if let Internable::String(ref name) = key {
                                if let Some(channel) = clients2.lock().unwrap().get(name) {
                                    channel.send(RunLoopMessage::RemoteTransaction(changes)).unwrap();
                                } else {
                                    panic!("Failed to send remote TX to nonexistent or unregistered client: '{}'", &name);
                                }
                            }
                        }
                    }
                    RouterMessage::Local(name, changes) => {
                        if let Some(channel) = clients2.lock().unwrap().get(&name) {
                            channel.send(RunLoopMessage::Transaction(changes)).unwrap();
                        } else {
                            panic!("Failed to send local TX to nonexistent or unregistered client: '{}'", &name);
                        }
                    }
                }
            }
        });
        Router { outgoing, clients, manager }
    }

    pub fn register(&mut self, name:&str, channel: Sender<RunLoopMessage>) {
        self.manager.send(RunLoopMessage::Transaction(vec![
            RawChange { e: s(name), a: s("tag"), v: s("router/event/add-client"), n: s("router"), count: 1 },
            RawChange { e: s(name), a: s("name"), v: s(name), n: s("router"), count: 1 },
        ])).unwrap();
        self.clients.lock().unwrap().insert(name.to_string(), channel);
    }

    pub fn unregister(&mut self, name:&str) {
        self.manager.send(RunLoopMessage::Transaction(vec![
            RawChange { e: s(name), a: s("tag"), v: s("router/event/remove-client"), n: s("router"), count: 1 },
            RawChange { e: s(name), a: s("name"), v: s(name), n: s("router"), count: 1 },
        ])).unwrap();
        self.clients.lock().unwrap().remove(name);
    }

    pub fn get_channel(&self) -> Sender<RouterMessage> {
        self.outgoing.clone()
    }
}


//-------------------------------------------------------------------------
// Remote Watcher
//-------------------------------------------------------------------------

pub struct RemoteWatcher {
    name: String,
    me: Internable,
    router_channel: Sender<RouterMessage>
}

impl RemoteWatcher {
    pub fn new(me:&str, router: &Router) -> RemoteWatcher {
        RemoteWatcher {name: "eve/remote".to_string(), me: Internable::String(me.to_string()), router_channel: router.get_channel() }
    }

    fn to_raw_change(&self, interner:&mut Interner, _type:Internable, to:Interned, _for:Interned, entity:Interned, attribute:Interned, value:Interned) -> RawRemoteChange {
        RawRemoteChange {
            e: interner.get_value(entity).clone(),
            a: interner.get_value(attribute).clone(),
            v: interner.get_value(value).clone(),
            _for: interner.get_value(_for).clone(),
            _type,
            from: self.me.clone(),
            to: interner.get_value(to).clone(),
        }
    }

}

impl Watcher for RemoteWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        let mut changes = vec![];
        // Fields: [to, _for, entity, attribute, value, allow_removes (0 or 1)]
        for remove in diff.removes {
            if remove[5] == 1 {
                match remove.as_slice() {
                    &[to, _for, entity, attribute, value, _] => {
                        // println!("SEND REMOVE: ({:?}, {:?}, {:?}, {:?}, {:?})", to, _for, entity, attribute, value);
                        changes.push(self.to_raw_change(interner, Internable::String("remove".to_string()), to, _for, entity, attribute, value));
                    }
                    _ => panic!("Invalid remote watch")
                }

            }
        }
        for add in diff.adds {
            match add.as_slice() {
                &[to, _for, entity, attribute, value, _] => {
                    // println!("SEND ADD: ({:?}, {:?}, {:?}, {:?}, {:?})", to, _for, entity, attribute, value);
                    changes.push(self.to_raw_change(interner, Internable::String("add".to_string()), to, _for, entity, attribute, value));
                }
                _ => panic!("Invalid remote watch")
            }
        }
        self.router_channel.send(RouterMessage::Remote(changes)).unwrap();
    }
}
