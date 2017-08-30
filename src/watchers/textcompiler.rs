
use super::Watcher;
use super::super::ops::{Internable, Interned, Interner,
                        RunLoopMessage};
use compiler::parse_string;
use indexes::WatchDiff;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

//-------------------------------------------------------------------------
// Raw text eve compiler
//-------------------------------------------------------------------------

pub struct RawTextCompilerWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
    id_to_blocks: HashMap<Interned, Vec<String>>,
}

impl RawTextCompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>)
        -> RawTextCompilerWatcher {
        RawTextCompilerWatcher {
            name: "eve/text-compiler".to_string(),
            outgoing,
            id_to_blocks: HashMap::new(),
        }
    }
}

impl Watcher for RawTextCompilerWatcher {
    fn get_name(&self) -> String { self.name.clone() }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner: &mut Interner, diff: WatchDiff) {
        let mut removed_blocks = vec![];
        let mut added_blocks = vec![];

        for remove in diff.removes {
            if let &Internable::String(ref kind) =
                interner.get_value(remove[0])
            {
                match (kind.as_ref(), &remove[1..]) {
                    ("code", &[id, _]) => {
                        match self.id_to_blocks.get_mut(&id) {
                            Some(names) => {
                                removed_blocks.extend(names.drain(..));
                            }
                            _ => {}
                        }
                    }
                    _ => {
                        println!("Found other removal '{:?}'", remove)
                    }
                }
            }
        }

        for add in diff.adds {
            if let Internable::String(ref kind) =
                interner.get_value(add[0]).clone()
            {
                match (kind.as_ref(), &add[1..]) {
                    ("to-blocks", &[id, path, code]) => {
                        match interner.get_value(code).clone() {
                            Internable::String(ref s) => {
                                let blocks =
                                    parse_string(interner,
                                                 s,
                                                 &path.to_string(),
                                                 false);
                                let mut changes = vec![];
                                for block in blocks {
                                    block.to_portable(interner).to_raw_changes(&mut changes);
                                }
                                self.outgoing.send(RunLoopMessage::Transaction(changes));
                            }
                            _ => {}
                        }
                    }
                    ("code", &[id, code]) => {
                        match interner.get_value(code).clone() {
                            Internable::String(ref s) => {
                                let blocks =
                                    parse_string(interner,
                                                 s,
                                                 &format!("eve/raw-text/{:?}",
                                                          id),
                                                 false);
                                let names =
                                    self.id_to_blocks
                                        .entry(id)
                                        .or_insert_with(|| vec![]);
                                names.extend(blocks.iter().map(|x| {
                                    x.name.to_owned()
                                }));
                                added_blocks.extend(blocks);
                            }
                            _ => {}
                        }
                    }
                    _ => println!("Found other '{:?}'", add),
                }
            }
        }

        if added_blocks.len() > 0 || removed_blocks.len() > 0 {
            self.outgoing
                .send(RunLoopMessage::CodeTransaction(added_blocks,
                                                      removed_blocks))
                .unwrap();
        }
    }
}
