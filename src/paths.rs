use std::env::current_exe;
use std::fs::canonicalize;
use std::path::PathBuf;

extern crate term_painter;
use self::term_painter::Color::*;
use self::term_painter::ToStyle;


//-------------------------------------------------------------------------
// Path Management
//-------------------------------------------------------------------------

pub struct EvePaths<'a> {
    pub files: Vec<&'a str>,
    pub server_files: Vec<&'a str>,

    pub persist_path: Option<PathBuf>,
    pub libraries_path: Option<PathBuf>,
    pub programs_path: Option<PathBuf>,
}

impl<'a> EvePaths<'a> {
    pub fn new(clean: bool,
               files: Vec<&'a str>,
               server_files: Vec<&'a str>,
               maybe_persist: Option<&str>,
               maybe_libraries: Option<&str>,
               maybe_programs: Option<&str>)
        -> EvePaths<'a> {
        let persist_path = maybe_persist.map(PathBuf::from);
        let libraries_path = maybe_libraries.map(PathBuf::from)
                                            .or_else(|| if !clean {
            find_in_root("libraries")
        } else {
            None
        });
        let programs_path = maybe_programs.map(PathBuf::from)
                                          .or_else(|| if !clean {
            find_in_root("examples")
        } else {
            None
        });
        EvePaths {
            files,
            server_files,
            persist_path,
            libraries_path,
            programs_path,
        }
    }

    pub fn persist(&self) -> Option<&str> {
        self.persist_path
            .as_ref()
            .and_then(|ref path| path.to_str())
    }
    pub fn libraries(&self) -> Option<&str> {
        self.libraries_path
            .as_ref()
            .and_then(|ref path| path.to_str())
    }
    pub fn programs(&self) -> Option<&str> {
        self.programs_path
            .as_ref()
            .and_then(|ref path| path.to_str())
    }
}

fn find_in_root(dir: &str) -> Option<PathBuf> {
    let current = current_exe()
        .and_then(|path| canonicalize(path));
    match current {
        Ok(mut cur) => {
            loop {
                let cur_path = cur.join("libraries"); // @FIXME: Change this to some filename that'll uniquely signify the root.
                let dir_path = cur.join(dir);
                if cur_path.exists() && dir_path.exists() {
                    return Some(dir_path);
                }
                if !cur.pop() {
                    break;
                }
            }
        }
        _ => {}
    }
    println!("{} Unable to find directory '{}' in ancestor and no {} path specified. Running without {}.",
             BrightYellow.paint("WARN:"),
             dir,
             dir,
             dir);
    None
}
