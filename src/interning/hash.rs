use ahash::RandomState;

const K0: u64 = 42;
const K1: u64 = 42;
const K2: u64 = 42;
const K3: u64 = 42;

pub const fn new_random_state() -> RandomState {
    RandomState::with_seeds(K0, K1, K2, K3)
}