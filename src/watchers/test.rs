use std::sync::mpsc;

use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner};
use super::Watcher;

pub enum TestMessage {
    Fail,
    Success
}

//-------------------------------------------------------------------------
// Test Watcher
//-------------------------------------------------------------------------

pub struct TestWatcher {
    name: String,
    outgoing: mpsc::Sender<TestMessage>
}

impl TestWatcher {
    pub fn new(outgoing:mpsc::Sender<TestMessage>) -> TestWatcher {
        TestWatcher{name: "test".to_string(), outgoing}
    }
}


impl Watcher for TestWatcher {
    fn get_name(&self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            match &kind[..] {
                "success" => { self.outgoing.send(TestMessage::Success).expect("Unable to send test result (success)."); },
                "fail" => { self.outgoing.send(TestMessage::Fail).expect("Unable to send test result (fail)."); },
                _ => panic!("Unexpected test message type: '{}'", kind)
            }
        }
    }
}
