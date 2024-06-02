use byteorder::ByteOrder;
use crate::interning::herbrand_universe::{InternedConstantTerms, InternedTerms};

pub const TERM_BYTE_SIZE: usize = 24 / 8;
pub const ATOM_BYTE_SIZE: usize = TERM_BYTE_SIZE * 3;
pub type EncodedAtom = [u8; ATOM_BYTE_SIZE];
pub type EncodedFact = [u8; ATOM_BYTE_SIZE];
pub type ProjectedEncodedFact = [u8; ATOM_BYTE_SIZE];
pub type ProjectedEncodedAtom = [u8; ATOM_BYTE_SIZE];
pub type EncodedGoal = EncodedFact;
pub fn encode_fact(fact: &InternedConstantTerms) -> EncodedFact {
    let mut encoded_fact: EncodedFact = Default::default();

    for (idx, term_value) in fact.iter().enumerate() {
        let term_start = idx * 3;
        let mut term_bits = [0u8; 3];
        let proper_term_value = (*term_value as u32) << 1;
        byteorder::NativeEndian::write_u24(&mut term_bits, proper_term_value);

        encoded_fact[term_start + 0] = term_bits[0];
        encoded_fact[term_start + 1] = term_bits[1];
        encoded_fact[term_start + 2] = term_bits[2];
    }

    encoded_fact
}
pub fn encode_atom_terms(atom: &InternedTerms) -> EncodedAtom {
    let mut encoded_atom: EncodedAtom = Default::default();

    for (idx, (is_var, term)) in atom.iter().enumerate() {
        let term_start = idx * 3;
        let mut term_bits = [0u8; 3];
        let mut proper_term_value = (*term as u32) << 1;
        if *is_var {
            proper_term_value |= 1;
        }
        byteorder::NativeEndian::write_u24(&mut term_bits, proper_term_value);

        encoded_atom[term_start + 0] = term_bits[0];
        encoded_atom[term_start + 1] = term_bits[1];
        encoded_atom[term_start + 2] = term_bits[2];
    }

    encoded_atom
}

pub fn encode_goal(goal: &InternedConstantTerms) -> EncodedGoal {
    let mut encoded_goal: EncodedGoal = Default::default();

    for (idx, term_value) in goal.iter().enumerate() {
        if *term_value != 0 {
            let term_start = idx * 3;
            let mut term_bits = [0u8; 3];
            let proper_term_value = *term_value as u32;
            byteorder::NativeEndian::write_u24(&mut term_bits, proper_term_value);

            encoded_goal[term_start + 0] = term_bits[0];
            encoded_goal[term_start + 1] = term_bits[1];
            encoded_goal[term_start + 2] = term_bits[2];
        }
    }

    encoded_goal
}

pub fn project_encoded_fact(fact: &EncodedFact, column_set: &Vec<usize>) -> ProjectedEncodedFact {
    let mut projected_fact = *fact;
    for idx in 0..TERM_BYTE_SIZE {
        if !column_set.contains(&idx) {
            let term_start = idx * 3;
            projected_fact[term_start] = 0;
            projected_fact[term_start + 1] = 0;
            projected_fact[term_start + 2] = 0;
        };
    }

    projected_fact
}
pub fn project_encoded_atom(atom: &EncodedAtom) -> ProjectedEncodedAtom {
    let mut projected_atom = *atom;
    for idx in 0..TERM_BYTE_SIZE {
        let term_start = idx * 3;
        if projected_atom[term_start] & 1 == 1 {
            projected_atom[term_start] = 0;
            projected_atom[term_start + 1] = 0;
            projected_atom[term_start + 2] = 0;
        }
    }

    projected_atom
}
pub fn decode_fact(fact: EncodedAtom) -> InternedConstantTerms {
    let mut decoded_fact = [0; 3];
    for idx in 0..TERM_BYTE_SIZE {
        let term_start = idx * 3;
        let term_bytes = &fact[term_start..(term_start + (TERM_BYTE_SIZE))];
        let term = (byteorder::NativeEndian::read_u24(term_bytes)) >> 1;

        decoded_fact[idx] = term as usize;
    }

    decoded_fact
}

#[cfg(test)]
mod tests {
    use crate::rewriting::atom::{decode_fact, encode_atom_terms, encode_fact, project_encoded_atom, project_encoded_fact};

    #[test]
    fn test_encode_fact() {
        let fact = [1usize, 2usize, 3usize];
        let expected_encoded_first_term = 1u8 << 1;
        let expected_encoded_second_term = 2u8 << 1;
        let expected_encoded_third_term = 3u8 << 1;
        let expected_encoded_fact = [expected_encoded_first_term, 0, 0, expected_encoded_second_term, 0, 0, expected_encoded_third_term, 0, 0];

        assert_eq!(expected_encoded_fact, encode_fact(&fact));
        let fact_as_atom = [(false, fact[0]), (false, fact[1]), (false, fact[2])];

        assert_eq!(expected_encoded_fact, encode_atom_terms(&fact_as_atom));
    }

    #[test]
    fn test_encode_atom() {
        let atom = [(false, 1usize), (true, 2usize), (true, 3usize)];
        let expected_encoded_first_term = 1u8 << 1;
        let expected_encoded_second_term = (2u8 << 1) | 1;
        let expected_encoded_third_term = (3u8 << 1) | 1;
        let expected_encoded_atom = [expected_encoded_first_term, 0, 0, expected_encoded_second_term, 0, 0, expected_encoded_third_term, 0, 0];

        assert_eq!(expected_encoded_atom, encode_atom_terms(&atom));
    }

    #[test]
    fn test_decode_fact() {
        let expected_fact_0 = [1usize, 2usize, 3usize];
        let expected_fact_1 = [1usize, 2usize, 0];
        let expected_fact_2 = [1usize, 0, 0];

        let fact_0 = decode_fact(encode_fact(&expected_fact_0));
        let fact_1 = decode_fact(encode_fact(&expected_fact_1));
        let fact_2 = decode_fact(encode_fact(&expected_fact_2));

        assert_eq!(expected_fact_0, fact_0);
        assert_eq!(expected_fact_1, fact_1);
        assert_eq!(expected_fact_2, fact_2);
    }

    #[test]
    fn test_project_encoded_fact() {
        let fact = [1usize, 2usize, 3usize];
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
        let atom = [(false, 1usize), (true, 3usize), (true, 4usize)];
        let expected_projected_atom = [1usize, 0usize, 0usize];

        assert_eq!(expected_projected_atom, decode_fact(project_encoded_atom(&encode_atom_terms(&atom))));
    }
}