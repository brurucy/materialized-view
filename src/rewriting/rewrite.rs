use crate::interning::herbrand_universe::InternedTerm;
use crate::rewriting::atom::{EncodedAtom, TERM_COUNT_BITS, TERM_COUNT_MASK, TERM_BITS_MASK, TERM_BITS};

// TODO Turn this into [32; u8]
pub type EncodedRewrite = u128;
const VARIABLE_BITS: u128 = 3;
const SUBSTITUTION_COUNT_BITS: u128 = 3;
const SUBSTITUTION_COUNT_MASK: u128 = 7;
pub fn get_from_encoded_rewrite(rewrite: &EncodedRewrite, variable: &InternedTerm) -> Option<InternedTerm> {
    let current_sub_count = *rewrite & SUBSTITUTION_COUNT_MASK;
    for idx in 0..current_sub_count {
        let variable_start_position = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_BITS - 1) as u128)) * idx;
        let variable_end_position = variable_start_position + VARIABLE_BITS;

        let variable_range_start = (1 << (variable_start_position)) - 1;
        let variable_range_end = (1 << (variable_end_position)) - 1;

        let variable_mask = variable_range_start ^ variable_range_end;

        let variable_value = ((rewrite & variable_mask) >> variable_start_position) as InternedTerm;

        if variable_value == *variable {
            let constant_start_position = variable_end_position;
            let constant_end_position = constant_start_position + ((TERM_BITS - 1) as u128);

            let constant_range_start = (1 << (constant_start_position)) - 1;
            let constant_range_end = (1 << (constant_end_position)) - 1;

            let constant_mask = constant_range_start ^ constant_range_end;

            let constant_value = ((rewrite & constant_mask) >> constant_start_position) as InternedTerm;

            return Some(constant_value)
        }
    }

    None
}

pub fn apply_rewrite(rewrite: &EncodedRewrite, encoded_atom: &EncodedAtom) -> EncodedAtom {
    let mut encoded_atom_copy = *encoded_atom;
    let len = TERM_COUNT_MASK & encoded_atom;
    for idx in 0..len {
        let shift_amount = TERM_COUNT_BITS + (TERM_BITS * idx);
        let term = (encoded_atom >> shift_amount) & TERM_BITS_MASK;
        let is_term_var = term & 1 == 1;
        if is_term_var {
            if let Some(constant) = get_from_encoded_rewrite(rewrite, &((term >> 1) as usize)) {
                let wipe_range_start = (1 << (shift_amount)) - 1;
                let wipe_range_end = (1 << (shift_amount + TERM_BITS)) - 1;
                let wipe_mask = wipe_range_start ^ wipe_range_end;
                encoded_atom_copy &= !wipe_mask;
                encoded_atom_copy |= ((constant as u64) << 1) << shift_amount;

                continue;
            }
        }
    }

    encoded_atom_copy
}

pub type VariableTerm = InternedTerm;
pub type ConstantTerm = InternedTerm;
pub type Substitution = (VariableTerm, ConstantTerm);

pub fn add_substitution(rewrite: &mut EncodedRewrite, substitution: Substitution) {
    let current_sub_count = *rewrite & SUBSTITUTION_COUNT_MASK;
    let variable_shift_amount = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_BITS - 1) as u128)) * current_sub_count;
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
        let shift_amount = TERM_COUNT_BITS + (TERM_BITS * (idx as u64));
        let left_term = (left_atom >> shift_amount) & TERM_BITS_MASK;
        let right_constant = (right_fact >> shift_amount) & TERM_BITS_MASK;

        let is_left_term_var = left_term & 1 == 1;
        if is_left_term_var {
            let left_variable_name = (left_term >> 1) as InternedTerm;
            let right_constant_value = (right_constant >> 1) as InternedTerm;

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
    let mut rewrite = left_rewrite;
    for idx in 0..right_len {
        let variable_start_position = SUBSTITUTION_COUNT_BITS + (VARIABLE_BITS + ((TERM_BITS - 1) as u128)) * idx;
        let variable_end_position = variable_start_position + VARIABLE_BITS;

        let variable_range_start = (1 << (variable_start_position)) - 1;
        let variable_range_end = (1 << (variable_end_position)) - 1;

        let variable_mask = variable_range_start ^ variable_range_end;
        let variable_value = ((right_rewrite & variable_mask) >> variable_start_position) as InternedTerm;

        if get_from_encoded_rewrite(&left_rewrite, &variable_value).is_none() {
            let constant_start_position = variable_end_position;
            let constant_end_position = constant_start_position + ((TERM_BITS - 1) as u128);

            let constant_range_start = (1 << (constant_start_position)) - 1;
            let constant_range_end = (1 << (constant_end_position)) - 1;

            let constant_mask = constant_range_start ^ constant_range_end;

            let constant_value = ((right_rewrite & constant_mask) >> constant_start_position) as InternedTerm;
            add_substitution(&mut rewrite, (variable_value, constant_value));
        }
    }

    rewrite
}

#[cfg(test)]
mod tests {
    use crate::rewriting::atom::{decode_fact, encode_atom_terms, encode_fact};
    use crate::rewriting::rewrite::{add_substitution, apply_rewrite, EncodedRewrite, get_from_encoded_rewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

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
        let encoded_atom = encode_atom_terms(&[(true, 1), (true, 2), (false, 0)]);
        let mut rewrite = 0u128;
        add_substitution(&mut rewrite, (1, 50));
        add_substitution(&mut rewrite, (3, 53));

        let result = apply_rewrite(&rewrite, &encoded_atom);
        let result = apply_rewrite(&rewrite, &result);
        // Idempotency
        let result = apply_rewrite(&rewrite, &result);
        assert_eq!(decode_fact(result), [50, 2, 0]);
    }

    #[test]
    fn test_unify_encoded_atom_with_encoded_rewrite() {
        let encoded_atom_0 = encode_atom_terms(&[(false, 2), (true, 1), (false, 0)]);
        let encoded_atom_1 = encode_atom_terms(&[(true, 1), (false, 0), (false, 0)]);
        let encoded_fact = encode_fact(&[2, 3, 0]);

        let rewrite_0 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_1, encoded_fact);
        assert!(rewrite_0.is_none());
        let rewrite_1 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_0, encoded_fact);
        assert!(!rewrite_1.is_none());
        let application_0 = apply_rewrite(&rewrite_1.unwrap(), &encoded_atom_1);

        assert_eq!(application_0, encode_atom_terms(&[(false, 3), (false, 0), (false, 0)]))
    }

    #[test]
    fn test_merge_right_rewrite_into_left() {
        let encoded_atom_0 = encode_atom_terms(&[(false, 3), (true, 4), (true, 5)]);
        let encoded_atom_1 = encode_atom_terms(&[(true, 6), (false, 0), (false, 0)]);

        let encoded_fact_0 = encode_fact(&[3, 5, 7]);
        let encoded_fact_1 = encode_fact(&[8, 0, 0]);

        let rewrite_0 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_0, encoded_fact_0);
        let rewrite_1 = unify_encoded_atom_with_encoded_rewrite(encoded_atom_1, encoded_fact_1);

        let encoded_atom_2 = encode_atom_terms(&[(true, 4), (true, 5), (true, 6)]);
        let expected_encoded_fact = [5, 7, 8];

        let rewrite_2 = merge_right_rewrite_into_left(rewrite_0.unwrap(), rewrite_1.unwrap());
        assert_eq!(encode_fact(&expected_encoded_fact), apply_rewrite(&rewrite_2, &encoded_atom_2))
    }
}
