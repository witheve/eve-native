#![feature(test)]
#![feature(link_args)]

// #[link_args = "-s EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
extern {}

mod ops;
use ops::doit;

mod indexes;
mod lubm;
use lubm::tests::test_lubm;

fn main() {
    // doit();
    test_lubm()
}
