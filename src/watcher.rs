extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;
extern crate time;

use self::tokio_core::reactor::{Core, Remote};
use tokio_timer::*;
use futures::*;
use std::time::*;
use indexes::{WatchDiff};
use ops::{Internable, Interner, RawChange};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self};

pub trait Watcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff);
}

pub struct SystemTimerWatcher {
    remote: Remote,
    outgoing: SyncSender<Vec<RawChange>>,
}

impl SystemTimerWatcher {
    pub fn new(outgoing: SyncSender<Vec<RawChange>>) -> SystemTimerWatcher {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let remote = core.remote();
            sender.send(remote).unwrap();
            loop {
                core.turn(None);
            }
        });
        let remote = receiver.recv().unwrap();
        SystemTimerWatcher { remote, outgoing }
    }
}

impl Watcher for SystemTimerWatcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff) {
        for add in diff.adds {
            println!("timer: {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
            let resolution = Internable::to_number(interner.get_value(add[1])) as u64;
            let timer_id = interner.get_value(add[0]).clone();
            let id = Internable::String(format!("system/timer/change/{}", add[0]));
            let timer = Timer::default();
            let interval = timer.interval_at(Instant::now(),Duration::from_millis(resolution));
            let outgoing = self.outgoing.clone();
            let foo = interval.for_each(move |_| {
                let cur_time = time::now();
                // println!("It's time! {:?}", cur_time);
                let changes = vec![
                    RawChange {e: id.clone(), a: Internable::String("tag".to_string()), v: Internable::String("system/timer/change".to_string()), n: Internable::String("System/timer".to_string()), count: 1},
                    RawChange {e: id.clone(), a: Internable::String("for".to_string()), v: timer_id.clone(), n: Internable::String("System/timer".to_string()), count: 1},
                    RawChange {e: id.clone(), a: Internable::String("hours".to_string()), v: Internable::from_number(cur_time.tm_hour as f32), n: Internable::String("System/timer".to_string()), count: 1},
                    RawChange {e: id.clone(), a: Internable::String("minutes".to_string()), v: Internable::from_number(cur_time.tm_min as f32), n: Internable::String("System/timer".to_string()), count: 1},
                    RawChange {e: id.clone(), a: Internable::String("seconds".to_string()), v: Internable::from_number(cur_time.tm_sec as f32), n: Internable::String("System/timer".to_string()), count: 1},
                ];
                outgoing.send(changes).unwrap();
                future::ok::<(), TimerError>(())
            }).map_err(|_| {
                panic!("uh oh");
            });
            self.remote.spawn(|handle| {
                handle.spawn(foo);
                Ok(())
            })
        }
    }
}

pub struct PrintWatcher { }

impl Watcher for PrintWatcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff) {
        for add in diff.adds {
            println!("Printer: {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
    }
}

