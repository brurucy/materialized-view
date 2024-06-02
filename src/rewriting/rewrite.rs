use byteorder::ByteOrder;
use crate::interning::herbrand_universe::InternedTerm;
use crate::rewriting::atom::{EncodedAtom, TERM_COUNT_BITS, TERM_COUNT_MASK, TERM_BITS_MASK, TERM_BITS, TERM_VALUE_MASK};

pub type EncodedRewrite = [u8; 32];
const CONSTANT_TERM_BYTE_SIZE: u8 = 24 / 8;
const VARIABLE_PLUS_CONSTANT_BYTE_SIZE: u8 = CONSTANT_TERM_BYTE_SIZE + 1;
#[inline]
fn get_encoded_rewrite_len(encoded_rewrite: &EncodedRewrite) -> u8 {
    encoded_rewrite.get(0).copied().unwrap_or_default()
}
#[inline]
fn get_ith_substitution(encoded_rewrite: &EncodedRewrite, i: u8) -> (u8, [u8; CONSTANT_TERM_BYTE_SIZE as usize]) {
    let ith_variable_position = ((i * VARIABLE_PLUS_CONSTANT_BYTE_SIZE) + 1) as usize;
    let ith_constant_position_start = ith_variable_position + 1;

    let ith_variable = encoded_rewrite[ith_variable_position];
    let ith_constant: [u8; CONSTANT_TERM_BYTE_SIZE as usize] = [
        encoded_rewrite[ith_constant_position_start],
        encoded_rewrite[ith_constant_position_start+1],
        encoded_rewrite[ith_constant_position_start+2],
    ];

    (ith_variable, ith_constant)
}

pub fn get_from_encoded_rewrite(rewrite: &EncodedRewrite, variable: u8) -> Option<InternedTerm> {
    let current_sub_count = get_encoded_rewrite_len(rewrite);
    (0..current_sub_count)
        .find_map(|ith_substitution_variable_position| {
            let substitution = get_ith_substitution(rewrite, ith_substitution_variable_position);
            if substitution.0 == variable {
                return Some(byteorder::NativeEndian::read_u24(&substitution.1) as usize)
            }

            None
        })
}

#[inline]
fn get_ith_term(encoded_atom: &EncodedAtom, i: u64) -> (bool, u64) {
    let ith_term = (encoded_atom >> TERM_COUNT_BITS) >> (TERM_BITS * i);
    let is_var = (ith_term & 1) == 1;
    let interned_term = (ith_term >> 1) & TERM_VALUE_MASK;

    (is_var, interned_term)
}

pub fn apply_rewrite(rewrite: &EncodedRewrite, encoded_atom: &EncodedAtom) -> EncodedAtom {
    let mut encoded_atom_copy = *encoded_atom;
    let term_count = encoded_atom & TERM_COUNT_MASK;
    let current_sub_count = get_encoded_rewrite_len(rewrite);

    (0..term_count).for_each(|ith_term| {
        let current_term = get_ith_term(encoded_atom, ith_term);
        if current_term.0 {
            for ith in 0..current_sub_count {
                let (variable, constant) = get_ith_substitution(rewrite, ith);
                if variable == (current_term.1 as u8) {
                    let shift_amount = TERM_COUNT_BITS + (TERM_BITS * ith_term);
                    let wipe_range_start = (1 << (shift_amount)) - 1;
                    let wipe_range_end = (1 << (shift_amount + TERM_BITS)) - 1;
                    let wipe_mask = wipe_range_start ^ wipe_range_end;
                    encoded_atom_copy &= !wipe_mask;
                    encoded_atom_copy |= ((byteorder::NativeEndian::read_u24(&constant) as u64) << 1) << shift_amount;
                    break;
                };
            }
        };
    });

    encoded_atom_copy
}

pub type VariableTerm = u8;
pub type ConstantTerm = [u8; 3];
pub type Substitution = (VariableTerm, ConstantTerm);

pub fn add_substitution(rewrite: &mut EncodedRewrite, substitution: Substitution) {
    let len = rewrite[0];
    // 0 - 1, 2, 3, 4
    // 1 - 5, 6, 7, 8
    // 2 - 9, 10, 11, 12
    // 3 - 13, 14, 15, 16
    // 4 - 17, 18, 19, 20
    let ith_variable_position = ((len * VARIABLE_PLUS_CONSTANT_BYTE_SIZE) + 1) as usize;
    let ith_constant_position_start = ith_variable_position + 1;
    rewrite[ith_variable_position] = substitution.0 as u8;

    let constant = substitution.1;
    rewrite[ith_constant_position_start] = constant[0];
    rewrite[ith_constant_position_start + 1] = constant[1];
    rewrite[ith_constant_position_start + 2] = constant[2];
    rewrite[0] += 1;
}

pub fn unify_encoded_atom_with_encoded_rewrite(left_atom: EncodedAtom, right_fact: EncodedAtom) -> Option<EncodedRewrite> {
    let left_len = TERM_COUNT_MASK & left_atom;
    let right_len = TERM_COUNT_MASK & right_fact;
    if left_len != right_len {
        return None;
    }
    let mut rewrite = Default::default();
    for idx in 0..(left_len as usize) {
        let shift_amount = TERM_COUNT_BITS + (TERM_BITS * (idx as u64));
        let left_term = (left_atom >> shift_amount) & TERM_BITS_MASK;
        let right_constant = (right_fact >> shift_amount) & TERM_BITS_MASK;

        let is_left_term_var = left_term & 1 == 1;
        if is_left_term_var {
            let left_variable_name = left_term >> 1;
            let right_constant_value = (right_constant >> 1) as u32;
            let mut right_constant = [0; 3];
            byteorder::NativeEndian::write_u24(&mut right_constant, right_constant_value);

            add_substitution(&mut rewrite, (left_variable_name as u8, right_constant));

            continue;
        }

        if left_term != right_constant {
            return None;
        }
    }


    Some(rewrite)
}

pub fn merge_right_rewrite_into_left(left_rewrite: EncodedRewrite, right_rewrite: EncodedRewrite) -> EncodedRewrite {
    let right_len = get_encoded_rewrite_len(&right_rewrite);
    let mut rewrite = left_rewrite;
    for idx in 0..right_len {
        let (variable, constant) = get_ith_substitution(&right_rewrite, idx);
        if get_from_encoded_rewrite(&left_rewrite, variable).is_none() {
            add_substitution(&mut rewrite, (variable, constant));
        }
    }

    rewrite
}

#[cfg(test)]
mod tests {
    use byteorder::ByteOrder;
    use crate::rewriting::atom::{decode_fact, encode_atom_terms, encode_fact};
    use crate::rewriting::rewrite::{add_substitution, apply_rewrite, get_from_encoded_rewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

    #[test]
    fn test_add_substitution() {
        let mut rewrite = Default::default();
        let constant_one = 100u32;
        let mut bytes_one = [0; 3];
        byteorder::NativeEndian::write_u24(&mut bytes_one, constant_one);

        let constant_two = 200u32;
        let mut bytes_two = [0; 3];
        byteorder::NativeEndian::write_u24(&mut bytes_two, constant_two);

        add_substitution(&mut rewrite, (1u8, bytes_one));
        add_substitution(&mut rewrite, (2u8, bytes_two));

        assert_eq!(get_from_encoded_rewrite(&rewrite, 1u8), Some(100));
        assert_eq!(get_from_encoded_rewrite(&rewrite, 2u8), Some(200));
        assert_eq!(get_from_encoded_rewrite(&rewrite, 3u8), None);
    }

    #[test]
    fn test_apply_rewrite() {
        let encoded_atom = encode_atom_terms(&[(true, 1), (true, 2), (false, 0)]);
        let mut rewrite = Default::default();
        let constant_one = 50u32;
        let mut bytes_one = [0; 3];
        byteorder::NativeEndian::write_u24(&mut bytes_one, constant_one);

        let constant_two = 53u32;
        let mut bytes_two = [0; 3];
        byteorder::NativeEndian::write_u24(&mut bytes_two, constant_two);

        add_substitution(&mut rewrite, (1, bytes_one));
        add_substitution(&mut rewrite, (3, bytes_two));

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
        let expected_encoded_fact = encode_fact(&[5, 7, 8]);

        let rewrite_2 = merge_right_rewrite_into_left(rewrite_0.unwrap(), rewrite_1.unwrap());
        assert_eq!(expected_encoded_fact, apply_rewrite(&rewrite_2, &encoded_atom_2))
    }
}
