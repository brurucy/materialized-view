use std::any::Any;
use std::hash::Hash;
use ahash::RandomState;

type FactData = [Option<Box<dyn Any>>; 3];
pub type FactIR = [u64; 3];
pub struct Fact { pub(crate) fact_ir: FactIR, pub(crate) fact_data: FactData }

impl<T: 'static> From<(T,)> for Fact where T: Hash {
    fn from(value: (T,)) -> Self {
        return Self { fact_ir: [RandomState::new().hash_one(&value.0), 0, 0], fact_data: [ Some(Box::new(value.0)), None, None ] }
    }
}

impl<T, R> From<(T, R)> for Fact where T: Hash, R: Hash {
    fn from(value: (T, R)) -> Self {
        let rs = RandomState::new();
        let first = rs.hash_one(&value.0);
        let second = rs.hash_one(&value.1);

        return Self { fact_ir: [first, second, 0], fact_data: [ Some(Box::new(first)), Some(Box::new(second)), None ] }
    }
}

impl<T, R, S> From<(T, R, S)> for Fact where T: Hash, R: Hash, S: Hash {
    fn from(value: (T, R, S)) -> Self {
        let rs = RandomState::new();
        let first = rs.hash_one(&value.0);
        let second = rs.hash_one(&value.1);
        let third = rs.hash_one(&value.2);

        return Self { fact_ir: [first, second, third], fact_data: [ Some(Box::new(first)), Some(Box::new(second)), Some(Box::new(third)) ] }
    }
}

#[cfg(test)]
mod tests {
    use crate::builders::fact::Fact;

    #[test]
    fn test_fact_builder() {
        let fact_builder = Fact::from((&"a", 1));
    }
}