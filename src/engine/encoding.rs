use datalog_syntax::AnonymousGroundAtom;
use crate::engine::rewrite::{InternedTerm, Rewrite};

// 0 is a canary value for terms!
pub type EncodedAtom = u64;
const TERM_COUNT_BITS: u64 = 2;
const TERM_COUNT_MASK: u64 = TERM_COUNT_BITS + 1;
const TERM_VALUE_BITS: u64 = 20;
const TERM_VALUE_MASK: u64 = (1 << 19) - 1;
pub fn encode_fact(value: &AnonymousGroundAtom) -> EncodedAtom {
    let len = value.len() as u64;
    let mut encoded_fact = len;

    for (idx, term_value) in value.iter().enumerate() {
        let term_bits = ((*term_value as u64) & TERM_VALUE_MASK) << 1;
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * (idx as u64));

        encoded_fact |= term_bits << shift_amount;
    }

    encoded_fact
}

pub fn encode_atom(value: &Vec<InternedTerm>) -> EncodedAtom {
    let mut encoded_atom = 0;
    let len = value.len() as u64;
    encoded_atom |= len;

    for (idx, term) in value.iter().enumerate() {
        let (term_value, is_var) = match term {
            InternedTerm::Variable(var_symbol) => (*var_symbol as u64, true),
            InternedTerm::Constant(constant_value) => (*constant_value as u64, false),
        };

        let mut term_bits = (term_value & TERM_VALUE_MASK) << 1;
        if is_var {
            term_bits |= 1;
        }
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * (idx as u64));

        encoded_atom |= term_bits << shift_amount;
    }

    encoded_atom
}
const TERM_KIND_AND_VALUE_MASK: u64 = (1 << 20) - 1;
pub fn unify_encoded_atom(left_atom: EncodedAtom, right_fact: EncodedAtom) -> Option<Rewrite> {
    let left_len = TERM_COUNT_MASK & left_atom;
    let right_len = TERM_COUNT_MASK & right_fact;
    if left_len != right_len {
        return None;
    }

    let mut rewrite: Rewrite = Default::default();
    for idx in 0..(left_len as usize) {
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * (idx as u64));
        let left_term = (left_atom >> shift_amount) & TERM_KIND_AND_VALUE_MASK;
        let right_constant = (right_fact >> shift_amount) & TERM_KIND_AND_VALUE_MASK;

        let is_left_term_var = left_term & 1 == 1;
        if is_left_term_var {
            rewrite.insert(((left_term >> 1) as usize, (right_constant >> 1) as usize));

            continue;
        }

        if left_term != right_constant {
            return None;
        }
    }

    Some(rewrite)
}

pub fn project_encoded_fact(atom: &EncodedAtom, column_set: &Vec<usize>) -> EncodedAtom {
    let len = TERM_COUNT_MASK & atom;
    let mut projection_mask = 0;
    for idx in 0..(len as usize) {
        if !column_set.contains(&idx) {
            let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * idx as u64);

            let range_start = (1 << (shift_amount)) - 1;
            let range_end = (1 << (shift_amount + TERM_VALUE_BITS)) - 1;

            let term_mask = range_start ^ range_end;

            projection_mask |= atom & term_mask;
        }
    }

    atom ^ projection_mask
}

pub fn project_encoded_atom(atom: &EncodedAtom) -> EncodedAtom {
    let len = TERM_COUNT_MASK & atom;
    let mut projection_mask = 0;
    for idx in 0..(len as usize) {
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * idx as u64);
        let ith_bit = 1 << shift_amount;

        if (atom & ith_bit) != ith_bit {
            continue;
        }

        let range_start = ith_bit - 1;
        let range_end = (1 << (shift_amount + TERM_VALUE_BITS)) - 1;

        let term_mask = range_start ^ range_end;

        projection_mask |= atom & term_mask;
    }

    atom ^ projection_mask
}

pub fn decode_fact(fact: EncodedAtom) -> AnonymousGroundAtom {
    let len = (TERM_COUNT_MASK & fact) as usize;
    let mut decoded_fact = Vec::with_capacity(len);
    for idx in 0..len {
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * idx as u64);

        let range_start = (1 << (shift_amount)) - 1;
        let range_end = (1 << (shift_amount + TERM_VALUE_BITS)) - 1;

        let term_mask = range_start ^ range_end;
        let term = fact & term_mask;
        let shifted_term = term >> shift_amount >> 1;

        decoded_fact.push(shifted_term as usize);
    }

    decoded_fact
}

#[cfg(test)]
mod tests {
    use crate::engine::encoding::{decode_fact, encode_atom, encode_fact, project_encoded_atom, project_encoded_fact, unify_encoded_atom};
    use crate::engine::rewrite::{InternedTerm, Rewrite};

    #[test]
    fn test_encode_fact() {
        let fact = vec![1usize, 2usize, 3usize];
        let expected_encoded_length = 3u64;
        let expected_encoded_first_term = 1u64 << 3;
        let expected_encoded_second_term = 2u64 << 23;
        let expected_encoded_third_term = 3u64 << 43;
        let expected_encoded_fact = expected_encoded_length | expected_encoded_first_term | expected_encoded_second_term | expected_encoded_third_term;

        assert_eq!(expected_encoded_fact, encode_fact(&fact));

        let fact_as_atom = fact.iter().map(|constant| InternedTerm::Constant(*constant)).collect();
        assert_eq!(expected_encoded_fact, encode_atom(&fact_as_atom));
    }

    #[test]
    fn test_encode_atom() {
        let atom = vec![InternedTerm::Constant(1usize), InternedTerm::Variable(2usize), InternedTerm::Variable(3usize)];
        let expected_encoded_length = 3u64;
        let expected_encoded_first_term = 1u64 << 3;
        let expected_encoded_second_term = (2u64 << 23) | (1 << 22);
        let expected_encoded_third_term = (3u64 << 43) | (1 << 42);
        let expected_encoded_atom = expected_encoded_length | expected_encoded_first_term | expected_encoded_second_term | expected_encoded_third_term;

        assert_eq!(expected_encoded_atom, encode_atom(&atom));
    }

    #[test]
    fn test_unify() {
        let atom = vec![InternedTerm::Constant(1usize), InternedTerm::Variable(3usize), InternedTerm::Variable(4usize)];
        let fact = vec![1usize, 2usize, 3usize];
        let mut expected_rewrite = Rewrite::default();
        expected_rewrite.insert((3, 2));
        expected_rewrite.insert((4, 3));

        assert_eq!(expected_rewrite, unify_encoded_atom(encode_atom(&atom), encode_fact(&fact)).unwrap())
    }

    #[test]
    fn test_decode_fact() {
        let expected_fact_0 = vec![1usize, 2usize, 3usize];
        let expected_fact_1 = vec![1usize, 2usize];
        let expected_fact_2 = vec![1usize];

        assert_eq!(expected_fact_0, decode_fact(encode_fact(&expected_fact_0)));
        assert_eq!(expected_fact_1, decode_fact(encode_fact(&expected_fact_1)));
        assert_eq!(expected_fact_2, decode_fact(encode_fact(&expected_fact_2)));
    }

    #[test]
    fn test_project_encoded_fact() {
        let fact = vec![1usize, 2usize, 3usize];
        let encoded_fact = encode_fact(&fact);

        let projection_0 = vec![0, 1, 2];
        let expected_fact_0 = vec![1usize, 2usize, 3usize];

        let projection_1 = vec![0, 1];
        let expected_fact_1 = vec![1usize, 2usize, 0usize];

        let projection_2 = vec![0];
        let expected_fact_2 = vec![1usize, 0usize, 0usize];

        let projection_3 = vec![0, 2];
        let expected_fact_3 = vec![1usize, 0usize, 3usize];

        assert_eq!(expected_fact_0, decode_fact(project_encoded_fact(&encoded_fact, &projection_0)));
        assert_eq!(expected_fact_1, decode_fact(project_encoded_fact(&encoded_fact, &projection_1)));
        assert_eq!(expected_fact_2, decode_fact(project_encoded_fact(&encoded_fact, &projection_2)));
        assert_eq!(expected_fact_3, decode_fact(project_encoded_fact(&encoded_fact, &projection_3)));
    }

    #[test]
    fn test_project_encoded_atom() {
        let atom = vec![InternedTerm::Constant(1usize), InternedTerm::Variable(3usize), InternedTerm::Variable(4usize)];
        let expected_projected_atom = vec![1usize, 0usize, 0usize];

        assert_eq!(expected_projected_atom, decode_fact(project_encoded_atom(&encode_atom(&atom))));
    }
}
