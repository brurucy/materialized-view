use std::any::Any;
use std::hash::Hash;
use crate::interning::hash::new_random_state;

type FactData = [Option<Box<dyn Any>>; 3];
pub type FactIR = [u64; 3];
pub struct Fact { pub(crate) fact_ir: FactIR, pub(crate) fact_data: FactData }

impl<T: 'static> From<(T,)> for Fact where T: Hash {
    fn from(value: (T,)) -> Self {
        let rs = new_random_state();
        let first = rs.hash_one(&value.0);

        return Self { fact_ir: [first, 0, 0], fact_data: [ Some(Box::new(value.0)), None, None ] }
    }
}

impl<T: 'static, R: 'static> From<(T, R)> for Fact where T: Hash, R: Hash {
    fn from(value: (T, R)) -> Self {
        let rs = new_random_state();
        let first = rs.hash_one(&value.0);
        let second = rs.hash_one(&value.1);

        return Self { fact_ir: [first, second, 0], fact_data: [ Some(Box::new(value.0)), Some(Box::new(value.1)), None ] }
    }
}

impl<T: 'static, R: 'static, S: 'static> From<(T, R, S)> for Fact where T: Hash, R: Hash, S: Hash {
    fn from(value: (T, R, S)) -> Self {
        let rs = new_random_state();
        let first = rs.hash_one(&value.0);
        let second = rs.hash_one(&value.1);
        let third = rs.hash_one(&value.2);

        return Self { fact_ir: [first, second, third], fact_data: [ Some(Box::new(value.0)), Some(Box::new(value.1)), Some(Box::new(value.2)) ] }
    }
}