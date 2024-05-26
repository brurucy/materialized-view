use std::hash::Hash;
use crate::interning::hash::new_random_state;
use crate::rewriting::atom::{EncodedFact, EncodedGoal};

#[allow(dead_code)]
pub const ANY_VALUE: Option<()> = None;

type GoalIR = [u64; 3];

pub struct Goal { pub(crate) goal_ir: GoalIR }

impl<T> From<(Option<T>,)> for Goal where T: Hash {
    fn from(value: (Option<T>,)) -> Self {
        let rs = new_random_state();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0.unwrap()) };

        return Self { goal_ir: [first, 0, 0] }
    }
}

impl<T, R> From<(Option<T>, Option<R>)> for Goal where T: Hash, R: Hash {
    fn from(value: (Option<T>, Option<R>)) -> Self {
        let rs = new_random_state();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0.unwrap()) };
        let second = if value.1.is_none() { 0 } else { rs.hash_one(&value.1.unwrap()) };

        return Self { goal_ir: [first, second, 0] }
    }
}

impl<T, R, S> From<(Option<T>, Option<R>, Option<S>)> for Goal where T: Hash, R: Hash, S: Hash {
    fn from(value: (Option<T>, Option<R>, Option<S>)) -> Self {
        let rs = new_random_state();
        let first = if value.0.is_none() { 0 } else { rs.hash_one(&value.0.unwrap()) };
        let second = if value.1.is_none() { 0 } else { rs.hash_one(&value.1.unwrap()) };
        let third = if value.2.is_none() { 0 } else { rs.hash_one(&value.2.unwrap()) };

        return Self { goal_ir: [first, second, third] }
    }
}

pub(crate) fn pattern_match(goal: &EncodedGoal, fact: &EncodedFact) -> bool {
    (goal & fact) == *goal
}