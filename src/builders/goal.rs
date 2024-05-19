use std::hash::Hash;
use ahash::RandomState;
use crate::engine::storage::InternedConstantTerms;

pub const ANY_VALUE: Option<()> = None;

type GoalIR = [u64; 3];

pub struct Goal { pub(crate) goal_ir: GoalIR }

impl<T> From<(Option<T>,)> for Goal where T: Hash {
    fn from(value: (Option<T>,)) -> Self {
        let rs = RandomState::new();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0) };

        return Self { goal_ir: [first, 0, 0] }
    }
}

impl<T, R> From<(Option<T>, Option<R>)> for Goal where T: Hash, R: Hash {
    fn from(value: (Option<T>, Option<R>)) -> Self {
        let rs = RandomState::new();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0) };
        let second = if value.0.is_none() { 0 } else { rs.hash_one(&value.1) };

        return Self { goal_ir: [first, second, 0] }
    }
}

impl<T, R, S> From<(Option<T>, Option<R>, Option<S>)> for Goal where T: Hash, R: Hash, S: Hash {
    fn from(value: (Option<T>, Option<R>, Option<S>)) -> Self {
        let rs = RandomState::new();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0) };
        let second = if value.1.is_none() { 0 } else { rs.hash_one(&value.1) };
        let third = if value.2.is_none() { 0 } else { rs.hash_one(&value.2) };

        return Self { goal_ir: [first, second, third] }
    }
}

    pub(crate) fn pattern_match(resolved_goal: &InternedConstantTerms, target: &InternedConstantTerms) -> bool {
        let first_value = resolved_goal.get(0).unwrap();
        if *first_value != 0 {
            if first_value != &target[0] {
                return false
            }
        }

        let second_value = resolved_goal.get(1).unwrap();
        if *second_value != 0 {
            if second_value != &target[1] {
                return false
            }
        }

        let third_value = resolved_goal.get(2).unwrap();
        if *third_value != 0 {
            if third_value != &target[2] {
                return false
            }
        }

        true
    }

#[cfg(test)]
mod tests {
    use crate::builders::goal::{ANY_VALUE, Goal};

    #[test]
    fn test_goal_builder() {
        let goal = Goal::from((ANY_VALUE,));
    }
}