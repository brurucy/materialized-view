use std::hash::Hash;
use crate::interning::hash::new_random_state;
use crate::rewriting::atom::{EncodedFact, EncodedGoal, TERM_COUNT_BITS};

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

const TERM_LEN_MASK: u64 = (1 << TERM_COUNT_BITS) - 1;
const FIRST_CONSTANT_MASK: u64 = TERM_LEN_MASK ^ ((1 << 22) - 1);
const SECOND_CONSTANT_MASK: u64 = ((1 << 22) - 1) ^ ((1 << 42) - 1);
const THIRD_CONSTANT_MASK: u64 = ((1 << 42) - 1) ^ ((1 << 62) - 1);
pub(crate) fn pattern_match(goal: &EncodedGoal, fact: &EncodedFact) -> bool {
    let first_goal_constant = goal & FIRST_CONSTANT_MASK;
    let first_fact_constant = fact & FIRST_CONSTANT_MASK;
    if first_goal_constant != 0 && first_goal_constant != first_fact_constant {
        return false
    }

    let second_goal_constant = goal & SECOND_CONSTANT_MASK;
    let second_fact_constant = fact & SECOND_CONSTANT_MASK;
    if second_goal_constant != 0 && second_goal_constant != second_fact_constant {
        return false
    }

    let third_goal_constant = goal & THIRD_CONSTANT_MASK;
    let third_fact_constant = fact & THIRD_CONSTANT_MASK;
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
        let fact_three = encode_goal(&[467000usize, 510000, 511000]);

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