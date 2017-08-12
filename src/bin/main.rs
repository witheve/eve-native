extern {}

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use eve::ops::{ProgramRunner, Persister};
use eve::watchers::system::{SystemTimerWatcher, PanicWatcher};
use eve::watchers::console::{ConsoleWatcher, PrintDiffWatcher};
use eve::watchers::file::FileWatcher;

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
                          .arg(Arg::with_name("clean")
                               .short("C")
                               .long("Clean")
                               .help("Starts Eve with a clean database and no watchers (false)"))
                          .get_matches();

    let files = match matches.values_of("EVE_FILES") {
        Some(fs) => fs.collect(),
        None => vec![]
    };
    let persist = matches.value_of("persist");
    let clean = matches.is_present("clean");

    let mut runner = ProgramRunner::new();
    let outgoing = runner.program.outgoing.clone();
    if !clean {
        runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(FileWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(ConsoleWatcher::new()));
        runner.program.attach(Box::new(PrintDiffWatcher::new()));
        runner.program.attach(Box::new(PanicWatcher::new()));
    }

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
