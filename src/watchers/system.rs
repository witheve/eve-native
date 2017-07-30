extern crate time;

use super::super::indexes::{WatchDiff};
use super::super::ops::{Interned, Internable, Interner, RawChange, RunLoopMessage};
use std::sync::mpsc::{self, Sender};
use std::thread::{self};
use std::time::*;
use std::collections::{HashMap};
use std::collections::hash_map::{Entry};
use super::Watcher;

//-------------------------------------------------------------------------
// System Watcher
//-------------------------------------------------------------------------

pub struct SystemTimerWatcher {
    outgoing: Sender<RunLoopMessage>,
    timers: HashMap<Interned, (usize, Sender<()>)>
}

impl SystemTimerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> SystemTimerWatcher {
        SystemTimerWatcher { outgoing, timers: HashMap::new() }
    }
}

impl Watcher for SystemTimerWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for remove in diff.removes {
            if let Entry::Occupied(mut entry) = self.timers.entry(remove[1]) {
                let should_remove = {
                    let pair = entry.get_mut();
                    let ref mut count = pair.0;
                    if *count > 1 {
                        *count -= 1;
                        false
                    } else {
                        pair.1.send(()).unwrap();
                        true
                    }
                };
                if should_remove {
                    entry.remove_entry();
                }
            }
        }

        for add in diff.adds {
            if let Entry::Occupied(mut entry) = self.timers.entry(add[1]) {
                let ref mut count = entry.get_mut().0;
                *count += 1;
                continue;
            }

            println!("timer: {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
            let internable_resolution = interner.get_value(add[1]).clone();
            let resolution = Internable::to_number(&internable_resolution) as u64;
            let id = Internable::String(format!("system/timer/change/{}", add[0]));

            let duration = Duration::from_millis(resolution);
            let (sender, receiver) = mpsc::channel();
            let outgoing = self.outgoing.clone();
            self.timers.insert(add[1], (1, sender));

            thread::spawn(move || {
                let mut tick = 0;
                loop {
                    thread::sleep(duration);
                    if receiver.try_recv().is_ok() {
                        break;
                    }
                    let cur_time = time::now();
                    // println!("It's time! {:?}", cur_time);
                    let changes = vec![
                        RawChange {e: id.clone(), a: Internable::String("tag".to_string()), v: Internable::String("system/timer/change".to_string()), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("resolution".to_string()), v: internable_resolution.clone(), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("hour".to_string()), v: Internable::from_number(cur_time.tm_hour as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("minute".to_string()), v: Internable::from_number(cur_time.tm_min as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("second".to_string()), v: Internable::from_number(cur_time.tm_sec as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("tick".to_string()), v: Internable::from_number(tick as f32), n: Internable::String("System/timer".to_string()), count: 1},
                    ];
                    tick += 1;
                    match outgoing.send(RunLoopMessage::Transaction(changes)) {
                        Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    }
}

