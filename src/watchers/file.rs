use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{Sender};
use std::fs::File;
use std::io::Error;
use std::io::prelude::*;
use std::path::Path;
use super::Watcher;

pub struct FileWatcher {
    outgoing: Sender<RunLoopMessage>,
}

impl FileWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> FileWatcher {
        FileWatcher { outgoing }
    }
}

fn file_error(changes: &mut Vec<RawChange>, id: String, why: Error) {
    let err_id = Internable::String(format!("file/error/{}", id));
    changes.push(RawChange {e: err_id.clone(), a: Internable::String("tag".to_string()), v: Internable::String("file/error".to_string()), n: Internable::String("file/error".to_string()), count: 1});
    changes.push(RawChange {e: err_id.clone(), a: Internable::String("message".to_string()), v: Internable::String(why.to_string()), n: Internable::String("file/error".to_string()), count: 1});
    changes.push(RawChange {e: err_id.clone(), a: Internable::String("file".to_string()), v: Internable::String(id.to_string()), n: Internable::String("file/error".to_string()), count: 1});
}

impl Watcher for FileWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let record_id = Internable::to_string(interner.get_value(add[1]));
            let id = Internable::String(format!("file/{}/change/{}", kind, record_id));
            let raw_path = Internable::to_string(interner.get_value(add[2]));
            let path = Path::new(&raw_path[..]);
            let mut changes = vec![];
            match &kind[..] {
                "read" => {
                    match File::open(&path) {
                        Err(why) => file_error(&mut changes, record_id, why),
                        Ok(mut file) => {
                            let mut contents = String::new();
                            match file.read_to_string(&mut contents) {
                                Err(why) => file_error(&mut changes, record_id, why),
                                Ok(_) => {
                                    changes.push(RawChange {e: id.clone(), a: Internable::String("tag".to_string()), v: Internable::String("file/read/change".to_string()), n: Internable::String("file/read".to_string()), count: 1});
                                    changes.push(RawChange {e: id.clone(), a: Internable::String("file".to_string()), v: Internable::String(record_id.to_string()), n: Internable::String("file/read".to_string()), count: 1});
                                    changes.push(RawChange {e: id.clone(), a: Internable::String("contents".to_string()), v: Internable::String(contents.clone()), n: Internable::String("file/read".to_string()), count: 1});
                                },
                            }
                        },
                    };
                },
                "write" => {
                    let contents = Internable::to_string(interner.get_value(add[3]));
                    match File::create(&path) {
                        Err(why) => file_error(&mut changes, record_id, why),
                        Ok(ref mut file) => {
                            match file.write_all(contents.as_bytes()) {
                                Err(why) => file_error(&mut changes, record_id, why),
                                Ok(_) => (),
                            };
                        },
                    };
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
