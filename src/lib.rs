#![feature(slice_patterns)]
#![feature(box_patterns)]
#![feature(vec_remove_item)]
#![feature(conservative_impl_trait)]


// #[link_args = "-s EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
extern {}

#[macro_use]
extern crate lazy_static;

extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate rand;

extern crate unicode_segmentation;

pub mod ops;

#[macro_use]
pub mod combinators;

pub mod indexes;
pub mod compiler;
pub mod parser;
pub mod error;
pub mod solver;

pub mod numerics;

pub mod watchers;

#[macro_use]
pub mod test_util;
