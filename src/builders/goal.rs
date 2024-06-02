use std::hash::Hash;
use byteorder::ByteOrder;
use crate::interning::hash::reproducible_hash_one;
use crate::rewriting::atom::{EncodedFact, EncodedGoal};
use crate::rewriting::rewrite::get_ith_term;

#[allow(dead_code)]
pub const ANY_VALUE: Option<()> = None;

type GoalIR = [u64; 3];

pub struct Goal { pub(crate) goal_ir: GoalIR }

impl<T> From<(Option<T>,)> for Goal where T: Hash {
    fn from(value: (Option<T>,)) -> Self {
        let first = if value.0.is_none() { 0 } else { reproducible_hash_one(&value.0.unwrap()) };

        return Self { goal_ir: [first, 0, 0] }
    }
}

impl<T, R> From<(Option<T>, Option<R>)> for Goal where T: Hash, R: Hash {
    fn from(value: (Option<T>, Option<R>)) -> Self {
        let first = if value.0.is_none() { 0 } else { reproducible_hash_one(&value.0.unwrap()) };
        let second = if value.1.is_none() { 0 } else { reproducible_hash_one(&value.1.unwrap()) };

        return Self { goal_ir: [first, second, 0] }
    }
}

impl<T, R, S> From<(Option<T>, Option<R>, Option<S>)> for Goal where T: Hash, R: Hash, S: Hash {
    fn from(value: (Option<T>, Option<R>, Option<S>)) -> Self {
        let first = if value.0.is_none() { 0 } else { reproducible_hash_one(&value.0.unwrap()) };
        let second = if value.1.is_none() { 0 } else { reproducible_hash_one(&value.1.unwrap()) };
        let third = if value.2.is_none() { 0 } else { reproducible_hash_one(&value.2.unwrap()) };

        return Self { goal_ir: [first, second, third] }
    }
}

pub(crate) fn pattern_match(goal: &EncodedGoal, fact: &EncodedFact) -> bool {
    let first_goal_constant = byteorder::NativeEndian::read_u24(get_ith_term(goal, 0).1);
    let first_fact_constant = byteorder::NativeEndian::read_u24(get_ith_term(fact, 0).1) >> 1;
    if first_goal_constant != 0 && first_goal_constant != first_fact_constant {
        return false
    }

    let second_goal_constant = byteorder::NativeEndian::read_u24(get_ith_term(goal, 1).1);
    let second_fact_constant = byteorder::NativeEndian::read_u24(get_ith_term(fact, 1).1) >> 1;
    if second_goal_constant != 0 && second_goal_constant != second_fact_constant {
        return false
    }

    let third_goal_constant = byteorder::NativeEndian::read_u24(get_ith_term(goal, 2).1);
    let third_fact_constant = byteorder::NativeEndian::read_u24(get_ith_term(fact, 2).1) >> 1;
    if third_goal_constant != 0 && third_goal_constant != third_fact_constant {
        return false
    }

    true
}

#[cfg(test)]
mod tests {
    use crate::builders::goal::pattern_match;
    use crate::rewriting::atom::{encode_fact, encode_goal};

    #[test]
    fn test_pattern_match() {
        let goal_one = encode_goal(&[1usize, 0, 0]);
        let goal_two = encode_goal(&[0usize, 0, 0]);
        let goal_three = encode_goal(&[467000usize, 510000, 511000]);

        let fact_one = encode_fact(&[1usize, 4, 0]);
        let fact_two = encode_fact(&[3usize, 4, 0]);
        let fact_three = encode_fact(&[467000usize, 510000, 511000]);

        assert!(pattern_match(&goal_one, &fact_one));
        assert!(!pattern_match(&goal_one, &fact_two));
        assert!(!pattern_match(&goal_one, &fact_three));

        assert!(pattern_match(&goal_two, &fact_one));
        assert!(pattern_match(&goal_two, &fact_two));
        assert!(pattern_match(&goal_two, &fact_three));

        assert!(!pattern_match(&goal_three, &fact_one));
        assert!(!pattern_match(&goal_three, &fact_two));
        assert!(pattern_match(&goal_three, &fact_three));
    }
}