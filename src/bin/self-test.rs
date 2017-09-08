extern {}

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

extern crate walkdir;
use self::walkdir::WalkDir;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self};

use eve::paths::EvePaths;
use eve::ops::{DebugMode, ProgramRunner, RunLoopMessage};
use eve::watchers::system::{SystemTimerWatcher, PanicWatcher};
use eve::watchers::console::{ConsoleWatcher, PrintDiffWatcher};
use eve::watchers::test::{TestWatcher, TestMessage};

pub struct TestSuite {
    name: String,
    root: PathBuf,
    tests: Vec<PathBuf>,
    prepared: bool
}

impl TestSuite {
    pub fn new(name:&str) -> TestSuite {
        let root = PathBuf::from("tests/eve").canonicalize().expect("Unable to canonicalize path 'tests/eve'");
        TestSuite{name: name.to_owned(), root, tests: vec![], prepared: false}
    }

    pub fn load(&mut self, path:&Path) {
        self.prepared = false;
        let metadata = fs::metadata(path).expect(&format!("Invalid path: {:?}", path));
        if metadata.is_file() {
            self.tests.push(path.canonicalize().expect(&format!("Unable to canonicalize path '{:?}'", path)));

        } else if metadata.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_file() {
                    let ext = entry.path().extension().map(|x| x.to_str().unwrap());
                    match ext {
                        Some("eve") | Some("md") => {
                            self.tests.push(entry.path().canonicalize().expect(&format!("Unable to canonicalize path '{:?}'", entry.path())));
                        },
                        _ => {}
                    }
                }
            }
        }
    }

    pub fn prepare(&mut self) {
        self.tests.sort();
        self.prepared = true;
    }

    pub fn run<F>(&self, make_runner: F) where F: Fn(String, mpsc::Sender<TestMessage>) -> ProgramRunner {
        if !self.prepared { panic!("Unable to run unprepared test suite. Try calling `suite.prepare()`."); }
        println!("Running Test Suite {}", self.name);
        if self.tests.len() == 0 {
            println!("  WARN: No test files found to run.");
        } else {
            let mut prev_suite = "".to_owned();
            for test in self.tests.iter() {
                let name = self.test_name(test);
                let cur_suite = self.suite_name(test);
                if cur_suite != prev_suite {
                    println!("  # {}", cur_suite);
                    prev_suite = cur_suite;
                }

                print!("    {}...", name);

                let (outgoing, incoming) = mpsc::channel();

                let mut runner = make_runner(format!("Test {}::{}::{}", self.name, prev_suite, name), outgoing);
                runner.load(&test.display().to_string());
                let running = runner.run_quiet();
                match incoming.recv() {
                    Ok(TestMessage::Fail) => println!("FAIL"),
                    Ok(TestMessage::Success) => println!("SUCCESS"),
                    _ => println!("ERROR")
                }
                running.send(RunLoopMessage::Stop);
            }
        }
    }

    fn suite_name(&self, path:&PathBuf) -> String {
        path.strip_prefix(&self.root).expect(&format!("ERROR: Test '{:?}' not in suite root '{:?}'", path, self.root)).display().to_string()
    }

    fn test_name(&self, path:&PathBuf) -> String {
        path.file_stem().expect(&format!("Unable to print filename for test '{:?}'", path)).to_str().unwrap().to_string()
    }
}

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let matches = App::new("Eve Self Test")
        .version("0.4")
        .author("Kodowa Inc.")
        .about("Run the Eve self test suite")
        .arg(Arg::with_name("library-path")
             .short("L")
             .long("library-path")
             .value_name("PATH")
             .help("Override default library path")
             .takes_value(true))
        .arg(Arg::with_name("suite")
             .help("The suite folder(s) or test file(s) to run")
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
                                  matches.values_of("suite").map_or(vec![], |files| files.collect()),
                                  vec![],
                                  None,
                                  matches.value_of("libraries-path"),
                                  matches.value_of("programs-path"));

    let mut suite = TestSuite::new("");
    for file in eve_paths.files.iter() {
        suite.load(Path::new(file));
    }
    suite.prepare();

    let make_runner = |name:String, test_outgoing:mpsc::Sender<TestMessage>| {
        let mut runner = ProgramRunner::new(&name);
        matches.value_of("debug").map(|mode_name| runner.debug(match mode_name {
            "compile" => DebugMode::Compile,
            _ => panic!("Unknown debug mode '{:?}'.", mode_name)
        }));

        let outgoing = runner.program.outgoing.clone();
        if !clean {
            runner.program.attach_quiet(Box::new(SystemTimerWatcher::new(outgoing.clone())));
            // runner.program.attach_quiet(Box::new(FileWatcher::new(outgoing.clone())));
            runner.program.attach_quiet(Box::new(ConsoleWatcher::new()));
            runner.program.attach_quiet(Box::new(PrintDiffWatcher::new()));
            runner.program.attach_quiet(Box::new(PanicWatcher::new()));
        }

        runner.program.attach_quiet(Box::new(TestWatcher::new(test_outgoing)));
        runner.load(&format!("{}/test/test-harness.eve", eve_paths.programs().expect("Program Path required to load test harness")));

        // if let &Some(path) = &eve_paths.libraries() {
        //     runner.load(path);
        // }
        runner
    };

    suite.run(make_runner);

    //let running = runner.run();
    //running.wait();
}
