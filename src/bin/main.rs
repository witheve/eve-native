extern {}

extern crate eve;
extern crate tokio_timer;
extern crate futures;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use eve::ops::{ProgramRunner, Persister};
use eve::watcher::{SystemTimerWatcher, PrintWatcher, PrintDiffWatcher};

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let matches = App::new("Eve")
                          .version("0.4")
                          .author("Kodowa Inc.")
                          .about("Creates an instance of the Eve server")
                          .arg(Arg::with_name("persist")
                               .long("persist")
                               .value_name("FILE")
                               .help("Sets the name for the database to load from and write to")
                               .takes_value(true))
                          .arg(Arg::with_name("EVE_FILES")
                               .help("The eve files and folders to load")
                               .required(true)
                               .multiple(true))
                          .get_matches();

    let files = match matches.values_of("EVE_FILES") {
        Some(fs) => fs.collect(),
        None => vec![]
    };
    let persist = matches.value_of("persist");

    let mut runner = ProgramRunner::new();
    let outgoing = runner.program.outgoing.clone();
    runner.program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
    runner.program.attach("system/print", Box::new(PrintWatcher{}));
    runner.program.attach("system/print-diff", Box::new(PrintDiffWatcher{}));

    if let Some(persist_file) = persist {
        let mut persister = Persister::new(persist_file);
        persister.load(persist_file);
        runner.persist(&mut persister);
    }

    for file in files {
        runner.load(file);
    }

    let running = runner.run();
    running.wait();
}
