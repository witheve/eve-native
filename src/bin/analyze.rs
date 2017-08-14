extern {}

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use eve::ops::{Program};
use eve::compiler::{parse_file};
use eve::analyzer::{Analysis};

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

    let mut program = Program::new();

    let mut blocks = vec![];
    for file in files {
        blocks.extend(parse_file(&mut program.state.interner, file, true));
    }

    let mut analysis = Analysis::new();
    for block in blocks {
        analysis.block(block);
    }
    analysis.analyze();
}

