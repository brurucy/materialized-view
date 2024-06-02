use std::hash::{BuildHasher, Hash};
use ahash::{AHasher, RandomState};
use once_cell::sync::Lazy;

static GLOBAL_HASHER: Lazy<RandomState> = Lazy::new(|| Default::default());

pub fn reproducible_hash_one(x: impl Hash) -> u64 {
    GLOBAL_HASHER.hash_one(x)
}

pub fn new_random_state() -> AHasher {
    GLOBAL_HASHER.build_hasher()
}