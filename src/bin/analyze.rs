extern {}

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use eve::ops::{Program};
use eve::compiler::{parse_file};
use eve::analyzer::{Analysis, to_javascript};
use std::fs::File;
use std::io::prelude::*;
use std::process::Command;

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let matches = App::new("Eve")
                          .version("0.4")
                          .author("Kodowa Inc.")
                          .about("Creates an instance of the Eve server")
                          .arg(Arg::with_name("EVE_FILES")
                               .help("The eve files and folders to load")
                               .required(true)
                               .multiple(true))
                          .get_matches();

    let files = match matches.values_of("EVE_FILES") {
        Some(fs) => fs.collect(),
        None => vec![]
    };

    let mut program = Program::new("analyzer");

    let mut blocks = vec![];
    for file in files {
        blocks.extend(parse_file(&mut program.state.interner, file, true, false));
    }

    let mut analysis = Analysis::new(&mut program.state.interner);
    for block in blocks.iter() {
        analysis.block(block);
    }
    analysis.analyze(&program.state.interner);
    let ir = analysis.program_to_ir(&program.state.interner);
    println!("\n IR ------------------------------------------------------ ");
    println!("{:?}", ir);
    println!("\n JS ------------------------------------------------------ ");
    println!("{}", to_javascript(&ir));

    let mut file = File::create("graph.dot").unwrap();
    file.write_all(analysis.make_dot_chains().as_bytes()).unwrap();
    let output = Command::new("dot")
        .arg("-Tsvg")
        .arg("graph.dot")
        .output()
        .expect("failed to execute process");
    let mut file2 = File::create("graph.svg").unwrap();
    file2.write_all(&output.stdout).unwrap();
}

