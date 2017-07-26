#![feature(link_args)]
#![feature(dropck_eyepatch)]
#![feature(generic_param_attrs)]
#![feature(sip_hash_13)]
#![feature(core_intrinsics)]
#![feature(shared)]
#![feature(unique)]
#![feature(placement_new_protocol)]
#![feature(fused)]
#![feature(alloc)]
#![feature(slice_patterns)]
#![feature(allocator_api)]
#![feature(box_patterns)]

// #[link_args = "-s EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
extern {}

#[macro_use]
extern crate lazy_static;

extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate tokio_timer;
extern crate futures;

extern crate unicode_segmentation;

pub mod ops;

#[macro_use]
pub mod combinators;

pub mod indexes;
pub mod hash;
pub mod compiler;
pub mod parser;
pub mod error;

pub mod watcher;
pub mod numerics;

#[macro_use]
pub mod test_util;
