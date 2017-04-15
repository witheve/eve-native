#![feature(test)]
#![feature(link_args)]

// #[link_args = "-s EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
extern {}
use std::num::Wrapping;
use std::time::Instant;

mod ops;
use ops::doit;

mod indexes;
mod lubm;
use lubm::tests::lubm_1;

fn rand(rseed:u32) -> u32 {
	return ((Wrapping(rseed) * Wrapping(1103515245) + Wrapping(12345)) & Wrapping(0x7fffffff)).0;
}

fn main() {
    // doit();
    lubm_1()
}
