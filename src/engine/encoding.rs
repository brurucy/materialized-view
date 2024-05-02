use datalog_syntax::{AnonymousGroundAtom, TypedValue};
use crate::engine::rewrite::{Domain, InternedTerm, Rewrite, Substitution};

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

pub type EncodedRewrite = u128;
const VARIABLE_BITS: u128 = 3;
const SUBSTITUTION_COUNT_BITS: u128 = 3;
const SUBSTITUTION_COUNT_MASK: u128 = 7;
pub fn get_from_encoded_rewrite(rewrite: &EncodedRewrite, variable: &Domain) -> Option<TypedValue> {
    let current_sub_count = *rewrite & SUBSTITUTION_COUNT_MASK;
    for idx in 0..current_sub_count {
        let variable_start_position = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_VALUE_BITS - 1) as u128)) * idx;
        let variable_end_position = variable_start_position + VARIABLE_BITS;

        let variable_range_start = (1 << (variable_start_position)) - 1;
        let variable_range_end = (1 << (variable_end_position)) - 1;

        let variable_mask = variable_range_start ^ variable_range_end;

        let variable_value = ((rewrite & variable_mask) >> variable_start_position) as Domain;

        if variable_value == *variable {
            let constant_start_position = variable_end_position;
            let constant_end_position = constant_start_position + ((TERM_VALUE_BITS - 1) as u128);

            let constant_range_start = (1 << (constant_start_position)) - 1;
            let constant_range_end = (1 << (constant_end_position)) - 1;

            let constant_mask = constant_range_start ^ constant_range_end;

            let constant_value = ((rewrite & constant_mask) >> constant_start_position) as TypedValue;

            return Some(constant_value)
        }
    }

    None
}

pub fn apply_rewrite(rewrite: &EncodedRewrite, encoded_atom: &EncodedAtom) -> EncodedAtom {
    let mut encoded_atom_copy = *encoded_atom;
    let len = TERM_COUNT_MASK & encoded_atom;
    for idx in 0..len {
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * idx);
        let term = (encoded_atom >> shift_amount) & TERM_KIND_AND_VALUE_MASK;
        let is_term_var = term & 1 == 1;
        if is_term_var {
            if let Some(constant) = get_from_encoded_rewrite(rewrite, &((term >> 1) as usize)) {
                let wipe_range_start = (1 << (shift_amount)) - 1;
                let wipe_range_end = (1 << (shift_amount + TERM_VALUE_BITS)) - 1;
                let wipe_mask = wipe_range_start ^ wipe_range_end;
                encoded_atom_copy &= !wipe_mask;
                encoded_atom_copy |= ((constant as u64) << 1) << shift_amount;

                continue;
            }
        }
    }

    encoded_atom_copy
}

pub fn add_substitution(rewrite: &mut EncodedRewrite, substitution: Substitution) {
    let current_sub_count = *rewrite & SUBSTITUTION_COUNT_MASK;
    let variable_shift_amount = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_VALUE_BITS - 1) as u128)) * current_sub_count;
    let constant_shift_amount = variable_shift_amount + VARIABLE_BITS;

    *rewrite |= (substitution.0 as u128) << variable_shift_amount;
    *rewrite |= (substitution.1 as u128) << constant_shift_amount;

    let current_sub_count_plus_one = (*rewrite & SUBSTITUTION_COUNT_MASK) + 1;

    *rewrite >>= SUBSTITUTION_COUNT_BITS;
    *rewrite <<= SUBSTITUTION_COUNT_BITS;
    *rewrite |= current_sub_count_plus_one;
}

pub fn unify_encoded_atom_with_encoded_rewrite(left_atom: EncodedAtom, right_fact: EncodedAtom) -> Option<EncodedRewrite> {
    let left_len = TERM_COUNT_MASK & left_atom;
    let right_len = TERM_COUNT_MASK & right_fact;
    if left_len != right_len {
        return None;
    }
    let mut rewrite = 0u128;
    for idx in 0..(left_len as usize) {
        let shift_amount = TERM_COUNT_BITS + (TERM_VALUE_BITS * (idx as u64));
        let left_term = (left_atom >> shift_amount) & TERM_KIND_AND_VALUE_MASK;
        let right_constant = (right_fact >> shift_amount) & TERM_KIND_AND_VALUE_MASK;

        let is_left_term_var = left_term & 1 == 1;
        if is_left_term_var {
            let left_variable_name = (left_term >> 1) as Domain;
            let right_constant_value = (right_constant >> 1) as TypedValue;

            add_substitution(&mut rewrite, (left_variable_name, right_constant_value));

            continue;
        }

        if left_term != right_constant {
            return None;
        }
    }


    Some(rewrite)
}

pub fn merge_right_rewrite_into_left(left_rewrite: EncodedRewrite, right_rewrite: EncodedRewrite) -> EncodedRewrite {
    let right_len = SUBSTITUTION_COUNT_MASK & right_rewrite;
    let mut rewrite = left_rewrite.clone();
    for idx in 0..right_len {
        let variable_start_position = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_VALUE_BITS - 1) as u128)) * idx;
        let variable_end_position = variable_start_position + VARIABLE_BITS;

        let variable_range_start = (1 << (variable_start_position)) - 1;
        let variable_range_end = (1 << (variable_end_position)) - 1;

        let variable_mask = variable_range_start ^ variable_range_end;
        let variable_value = ((right_rewrite & variable_mask) >> variable_start_position) as Domain;

        if get_from_encoded_rewrite(&left_rewrite, &variable_value).is_none() {
            let constant_start_position = variable_end_position;
            let constant_end_position = constant_start_position + ((TERM_VALUE_BITS - 1) as u128);

            let constant_range_start = (1 << (constant_start_position)) - 1;
            let constant_range_end = (1 << (constant_end_position)) - 1;

            let constant_mask = constant_range_start ^ constant_range_end;

            let constant_value = ((right_rewrite & constant_mask) >> constant_start_position) as TypedValue;
            add_substitution(&mut rewrite, (variable_value, constant_value));
        }
    }

    rewrite
}

const GROUND_ATOM_MASK: u64 = (1 << 2) | (1 << 22) | (1 << 42);
pub fn is_encoded_atom_ground(atom: &EncodedAtom) -> bool {
    return (atom & GROUND_ATOM_MASK) == 0
}

#[cfg(test)]
mod tests {
    use crate::engine::encoding::{add_substitution, apply_rewrite, decode_fact, encode_atom, encode_fact, EncodedRewrite, get_from_encoded_rewrite, is_encoded_atom_ground, project_encoded_atom, project_encoded_fact, unify_encoded_atom, unify_encoded_atom_with_encoded_rewrite};
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
    fn test_unify_encoded_atom_to_encoded_rewrite() {
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

    #[test]
    fn test_add_substitution() {
        let mut rewrite: EncodedRewrite = 0;
        add_substitution(&mut rewrite, (1, 100));
        add_substitution(&mut rewrite, (2, 200));
        assert_eq!(get_from_encoded_rewrite(&rewrite, &1), Some(100));
        assert_eq!(get_from_encoded_rewrite(&rewrite, &2), Some(200));
        assert_eq!(get_from_encoded_rewrite(&rewrite, &3), None);
    }

    #[test]
    fn test_apply_rewrite() {
        let encoded_atom = encode_atom(&vec![InternedTerm::Variable(1), InternedTerm::Variable(2)]);
        let mut rewrite = 0u128;
        add_substitution(&mut rewrite, (1, 50));
        add_substitution(&mut rewrite, (3, 53));

        let result = apply_rewrite(&rewrite, &encoded_atom);
        // Idempotent
        let result = apply_rewrite(&rewrite, &result);
        let result = apply_rewrite(&rewrite, &result);
        assert_eq!(decode_fact(result), vec![50, 2]);
    }

    #[test]
    fn test_unification_failure() {
        let encoded_atom_0 = encode_atom(&vec![InternedTerm::Constant(2), InternedTerm::Variable(1)]);
        let encoded_atom_1 = encode_atom(&vec![InternedTerm::Variable(1)]);
        let encoded_fact = encode_fact(&vec![2, 3]);

        let rewrite_0 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_1, encoded_fact);
        assert!(rewrite_0.is_none());
        let rewrite_1 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_0, encoded_fact);
        assert!(!rewrite_1.is_none());
        let application_0 = apply_rewrite(&rewrite_1.unwrap(), &encoded_atom_1);
        println!("{:?}", decode_fact(application_0));
        assert_eq!(application_0, encode_atom(&vec![InternedTerm::Constant(3)]))
    }

    #[test]
    fn test_is_ground() {
        let encoded_atom_0 = encode_atom(&vec![InternedTerm::Constant(2), InternedTerm::Variable(1)]);
        assert!(!is_encoded_atom_ground(&encoded_atom_0));

        let encoded_atom_1 = encode_atom(&vec![InternedTerm::Variable(1)]);
        assert!(!is_encoded_atom_ground(&encoded_atom_1));

        let encoded_atom_2 = encode_atom(&vec![InternedTerm::Constant(2), InternedTerm::Constant(3)]);
        assert!(is_encoded_atom_ground(&encoded_atom_2));

        let encoded_fact = encode_fact(&vec![2, 3]);
        assert!(is_encoded_atom_ground(&encoded_fact));
    }
}
