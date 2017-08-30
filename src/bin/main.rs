extern {}

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use eve::paths::EvePaths;
use eve::ops::{DebugMode, ProgramRunner, Persister};
use eve::watchers::system::{SystemTimerWatcher, PanicWatcher};
use eve::watchers::console::{ConsoleWatcher, PrintDiffWatcher};
use eve::watchers::file::FileWatcher;
use eve::watchers::json::JsonWatcher;

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
                          .arg(Arg::with_name("library-path")
                               .short("L")
                               .long("library-path")
                               .value_name("PATH")
                               .help("Override default library path")
                               .takes_value(true))
                          .arg(Arg::with_name("EVE_FILES")
                               .help("The eve files and folders to load")
                               .required(true)
                               .multiple(true))
                          .arg(Arg::with_name("clean")
                               .short("C")
                               .long("Clean")
                               .help("Starts Eve with a clean database and no watchers (false)"))
                          .arg(Arg::with_name("debug")
                               .short("D")
                               .long("debug")
                               .value_name("MODE")
                               .help("Enable the specified debug mode. Options: ('compile')"))
                          .get_matches();

    let clean = matches.is_present("clean");

    let eve_paths = EvePaths::new(clean,
                                  matches.values_of("EVE_FILES").map_or(vec![], |files| files.collect()),
                                  matches.value_of("server-file").map_or(vec![], |file| vec![file]),
                                  matches.value_of("persist"),
                                  matches.value_of("libraries-path"),
                                  matches.value_of("programs-path"));

    let mut runner = ProgramRunner::new("main");
    matches.value_of("debug").map(|mode_name| runner.debug(match mode_name {
        "compile" => DebugMode::Compile,
        _ => panic!("Unknown debug mode '{:?}'.", mode_name)
    }));

    let outgoing = runner.program.outgoing.clone();
    if !clean {
        runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(FileWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(ConsoleWatcher::new()));
        runner.program.attach(Box::new(PrintDiffWatcher::new()));
        runner.program.attach(Box::new(JsonWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(PanicWatcher::new()));
    }

    if let Some(persist_file) = eve_paths.persist() {
        let mut persister = Persister::new(persist_file);
        persister.load(persist_file);
        runner.persist(&mut persister);
    }

    if let &Some(path) = &eve_paths.libraries() {
        runner.load(path);
    }
    for file in eve_paths.files.iter() {
        runner.load(file);
    }

    let running = runner.run();
    running.wait();
}
