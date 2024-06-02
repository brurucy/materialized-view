use byteorder::ByteOrder;
use crate::interning::herbrand_universe::InternedTerm;
use crate::rewriting::atom::{EncodedAtom, EncodedFact, TERM_BYTE_SIZE};

pub type EncodedRewrite = [u8; 32];
const VARIABLE_PLUS_CONSTANT_BYTE_SIZE: usize = TERM_BYTE_SIZE + 1;
#[inline]
fn get_encoded_rewrite_len(encoded_rewrite: &EncodedRewrite) -> u8 {
    encoded_rewrite.get(0).copied().unwrap_or_default()
}
#[inline]
fn get_ith_substitution(encoded_rewrite: &EncodedRewrite, i: u8) -> (u8, [u8; TERM_BYTE_SIZE]) {
    let ith_variable_position = ((i * (VARIABLE_PLUS_CONSTANT_BYTE_SIZE as u8)) + 1) as usize;
    let ith_constant_position_start = ith_variable_position + 1;

    let ith_variable = encoded_rewrite[ith_variable_position];
    let ith_constant: [u8; TERM_BYTE_SIZE] = [
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
pub fn get_ith_term(encoded_atom: &EncodedAtom, i: u64) -> (bool, &[u8]) {
    let term_start = (i * 3) as usize;
    let is_var = (encoded_atom[term_start] & 1) == 1;

    (is_var, &encoded_atom[term_start..(term_start + 3)])
}

pub fn apply_rewrite(rewrite: &EncodedRewrite, encoded_atom: &EncodedAtom) -> EncodedAtom {
    let mut encoded_atom_copy = *encoded_atom;
    let current_sub_count = get_encoded_rewrite_len(rewrite);

    (0..3).for_each(|ith_term| {
        let (is_var, current_term) = get_ith_term(encoded_atom, ith_term);
        if is_var {
            for ith in 0..current_sub_count {
                let (variable, constant) = get_ith_substitution(rewrite, ith);
                if variable == (current_term[0] >> 1) {
                    let term_start = (ith_term * 3) as usize;
                    encoded_atom_copy[term_start] = constant[0] << 1;
                    encoded_atom_copy[term_start + 1] = constant[1];
                    encoded_atom_copy[term_start + 2] = constant[2];
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
    let ith_variable_position = ((len * (VARIABLE_PLUS_CONSTANT_BYTE_SIZE as u8)) + 1) as usize;
    let ith_constant_position_start = ith_variable_position + 1;
    rewrite[ith_variable_position] = substitution.0 as u8;

    let constant = substitution.1;
    rewrite[ith_constant_position_start] = constant[0];
    rewrite[ith_constant_position_start + 1] = constant[1];
    rewrite[ith_constant_position_start + 2] = constant[2];
    rewrite[0] += 1;
}

fn get_atom_len(atom: EncodedAtom) -> usize {
    let is_first_term_not_empty = if (atom[0].count_ones() + atom[1].count_ones() + atom[2].count_ones()) > 0 { 1 } else { 0 };
    let is_second_term_not_empty = if (atom[3].count_ones() + atom[4].count_ones() + atom[5].count_ones()) > 0 { 1 } else { 0 };
    let is_third_term_not_empty = if (atom[6].count_ones() + atom[7].count_ones() + atom[8].count_ones()) > 0 { 1 } else { 0 };

    is_first_term_not_empty + is_second_term_not_empty + is_third_term_not_empty
}

pub fn unify_encoded_atom_with_encoded_rewrite(left_atom: EncodedAtom, right_fact: EncodedFact) -> Option<EncodedRewrite> {
    let left_len = get_atom_len(left_atom);
    let right_len = get_atom_len(right_fact);
    if left_len != right_len {
        return None;
    }
    let mut rewrite = Default::default();
    for idx in 0..left_len {
        let (is_left_term_var, left_term) = get_ith_term(&left_atom, idx as u64);
        let (_, right_term) = get_ith_term(&right_fact, idx as u64);

        if is_left_term_var {
            let left_variable_name = left_term[0] >> 1;
            let mut right_constant = ConstantTerm::try_from(right_term).unwrap();
            right_constant[0] >>= 1;

            add_substitution(&mut rewrite, (left_variable_name, right_constant));

            continue;
        }

        if left_term != right_term {
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
