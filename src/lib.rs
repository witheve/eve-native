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

// #[link_args = "-s EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
extern {}

#[macro_use]
extern crate lazy_static;

extern crate serde;

extern crate tokio_timer;
extern crate futures;

pub mod ops;

#[macro_use]
pub mod combinators;

pub mod indexes;
pub mod hash;
pub mod compiler;
pub mod parser;

pub mod watcher;
pub mod numerics;

