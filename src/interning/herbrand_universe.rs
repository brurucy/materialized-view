use std::any::Any;
use indexmap::IndexMap;
use crate::builders::fact::Fact;
use crate::builders::rule::{Atom, Rule};
use crate::engine::storage::InternedConstantTerms;

const MAXIMUM_LEN: usize = 1 << 19;

pub type Interner = IndexMap<u64, Box<dyn Any>>;

#[derive(Default)]
pub struct InternmentLayer {
    inner: Interner
}

pub type InternedTerms = [(bool, usize); 3];
pub type InternedAtom = (u64, InternedTerms);
pub type InternedRule = (u64, InternedAtom, Vec<InternedAtom>);

impl InternmentLayer {
    pub fn push(&mut self, hash: u64, data: Box<dyn Any>) -> usize {
        self.inner.insert_full(hash, data).0
    }
    pub fn intern_fact(&mut self, fact: Fact) -> InternedConstantTerms {
        let mut interned_constant_terms = [0; 3];

        let first_key = fact.fact_ir[0];
        let potential_first_value = fact.fact_data[0];
        if let Some(first_value) = potential_first_value {
            interned_constant_terms[0] = self.push(first_key, first_value);
        }

        let second_key = fact.fact_ir[1];
        let potential_second_value = fact.fact_data[1];
        if let Some(second_value) = potential_second_value {
            interned_constant_terms[1] = self.push(second_key, second_value);
        }
        
        let third_key = fact.fact_ir[2];
        let potential_third_value = fact.fact_data[2];
        if let Some(third_value) = potential_third_value {
            interned_constant_terms[2] = self.push(third_key, third_value);
        }

        interned_constant_terms
    }
    pub fn intern_atom(&mut self, atom: Atom) -> InternedAtom {
        let first = atom.atom_ir[0];
        let second = atom.atom_ir[1];
        let third = atom.atom_ir[2];

        let interned_first = if first.0 { (true, first.1 as usize) } else {
            (false, self.push(first.1)) 
        };
        let interned_second = if second.0 { (true, second.1 as usize) } else {
            (false, self.push(second.1)) 
        };
        let interned_third = if third.0 { (true, third.1 as usize) } else {
            (false, self.push(third.1)) 
        };

        let interned_atom_ir = [ interned_first, interned_second, interned_third ];

        (atom.symbol, interned_atom_ir)
    }
    pub fn intern_rule(&mut self, rule: Rule) -> InternedRule {
        (rule.id, self.intern_atom(rule.head), rule.body.into_iter().map(|atom| self.intern_atom(atom)).collect())
    }
    pub fn resolve_fact(&self, fact: Fact) -> Option<InternedConstantTerms> {
        let mut resolved_fact = [0; 3];
        for i in 0..3usize {
            if let Some(resolved_constant) = self.inner.get_index_of(&fact.fact_ir[i]) {
                resolved_fact[i] = resolved_constant;
            } else {
                return None
            };
        }

        Some(resolved_fact)
    }
    pub fn resolve_atom(&self, atom: Atom) -> Option<InternedAtom> {
        let mut resolved_atom = [(false, 0); 3];
        for i in 0..3usize {
            if !atom.atom_ir[i].0 {
                if let Some(resolved_constant) = self.inner.get_index_of(&atom.atom_ir[i].1) {
                    resolved_atom[i] = (false, resolved_constant);
                    continue;
                } else {
                    return None
                }
            }

            resolved_atom[i] = (atom.atom_ir[i].0, atom.atom_ir[i].1 as usize)
        }

        Some((atom.symbol, resolved_atom))
    }
    pub fn resolve_rule(&self, rule: Rule) -> Option<InternedRule> {
        if let Some(resolved_head) = self.resolve_atom(rule.head) {
            let mut resolved_body = vec![];

            for body_atom in rule.body {
                if let Some(resolved_body_atom) = self.resolve_atom(body_atom) {
                    resolved_body.push(resolved_body_atom);
                } else {
                    return None
                }
            }

            return Some((rule.id, resolved_head, resolved_body))
        }

        None
    }
    pub fn new() -> Self {
        let mut inner: Interner = Default::default();
        inner.insert(0);

        Self { inner }
    }
}