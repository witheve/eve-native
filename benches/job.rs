#![feature(test)]

extern crate test;
extern crate time;
extern crate eve;
extern crate csv;

use eve::ops::{Program};
use test::Bencher;


pub fn load(program:&mut Program) {
    let mut eavs = vec![];
    macro_rules! n (($i:expr) => ({ program.interner.number($i as f32) }));
    macro_rules! s (($i:expr) => ({ program.interner.string(&$i) }));
    macro_rules! eav (($e:expr,$a:expr,$v:expr) => ({ eavs.push(($e,$a,$v)) }));
    macro_rules! csv_eav (($rec:ident, $attr:expr, $idx:tt, String) => { eav!(n!($rec.0), s!($attr), s!($rec.$idx)); };
                          ($rec:ident, $attr:expr, $idx:tt, f32) => { eav!(n!($rec.0), s!($attr), n!($rec.$idx)); };
                          ($rec:ident, $attr:expr, $idx:tt, i32) => { eav!(n!($rec.0), s!($attr), n!($rec.$idx)); };
                          );
    macro_rules! csv (($file:expr, $tag:expr, ($(($idx:tt, $attr:expr,$type:tt)),*)) => ({
        let mut rdr = csv::Reader::from_file("./data/imdb/".to_string()+$file).unwrap();
        for record in rdr.decode() {
            let record:(u32 $(,$type)*) = record.unwrap();
            eav!(n!(record.0), s!("tag"), s!($tag));
            $( csv_eav!(record, $attr, $idx, $type); )*
        }
    }));

    csv!("keyword.csv", "keyword", ((1, "keyword", String), (2, "phonetic-code", String)));
}

#[bench]
pub fn job_4b(b: &mut Bencher) {
    let mut program = Program::new();
    load(&mut program);
}

