use std::any::Any;
use indexmap::IndexMap;
use crate::builders::fact::Fact;
use crate::builders::goal::Goal;
use crate::builders::rule::{Atom, Rule};
use crate::engine::storage::InternedConstantTerms;

const MAXIMUM_LEN: usize = 1 << 19;

pub type Interner = IndexMap<u64, Box<dyn Any>>;

#[derive(Default)]
pub struct InternmentLayer {
    inner: Interner
}

pub type InternedTerm = usize;
pub type InternedTerms = [(bool, InternedTerm); 3];
pub type InternedAtom = (u64, InternedTerms);
pub type InternedRule = (u64, InternedAtom, Vec<InternedAtom>);

impl InternmentLayer {
    pub fn resolve_hash<T: 'static>(&self, hash: u64) -> Option<&T> {
        if let Some(hash_value) = self.inner.get(&hash) {
            return Some(hash_value.downcast_ref::<T>().unwrap())
        }

        None
    }
    pub fn push(&mut self, hash: u64, data: Box<dyn Any>) -> usize {
        self.inner.insert_full(hash, data).0 + 1
    }
    pub fn intern_fact(&mut self, fact: Fact) -> InternedConstantTerms {
        let mut interned_constant_terms = [0; 3];
        let mut fact = fact;

        let first_key = *fact.fact_ir.get(0).unwrap();
        if let Some(first_value) = fact.fact_data[0].take() {
            interned_constant_terms[0] = self.push(first_key, first_value);
        }

        let second_key = *fact.fact_ir.get(1).unwrap();
        if let Some(second_value) = fact.fact_data[1].take() {
            interned_constant_terms[1] = self.push(second_key, second_value);
        }
        
        let third_key = *fact.fact_ir.get(2).unwrap();
        if let Some(third_value) = fact.fact_data[2].take() {
            interned_constant_terms[2] = self.push(third_key, third_value);
        }

        interned_constant_terms
    }
    pub fn intern_atom(&mut self, atom: Atom) -> InternedAtom {
        let mut interned_atom_ir = [ (false, 0); 3 ];
        let mut atom = atom;

        if let Some(first_value) = atom.atom_data[0].take() {
            let first_key = atom.atom_ir[0];
            if !first_key.0 {
                interned_atom_ir[0] = (false, self.push(first_key.1, first_value));
            } else {
                interned_atom_ir[0] = (true, first_key.1 as usize)
            }
        }

        if let Some(second_value) = atom.atom_data[1].take() {
            let second_key = atom.atom_ir[1];
            if !second_key.0 {
                interned_atom_ir[1] = (false, self.push(second_key.1, second_value));
            } else {
                interned_atom_ir[0] = (true, second_key.1 as usize)
            }
        }

        if let Some(third_value) = atom.atom_data[2].take() {
            let third_key = atom.atom_ir[2];
            if !third_key.0 {
                interned_atom_ir[2] = (false, self.push(third_key.1, third_value));
            } else {
                interned_atom_ir[2] = (true, third_key.1 as usize);
            }
        }

        (atom.symbol, interned_atom_ir)
    }
    pub fn intern_rule(&mut self, rule: Rule) -> InternedRule {
        (rule.id, self.intern_atom(rule.head), rule.body.into_iter().map(|atom| self.intern_atom(atom)).collect())
    }
    pub fn resolve_fact(&self, fact: Fact) -> Option<InternedConstantTerms> {
        let mut resolved_fact = [0; 3];
        for i in 0..3usize {
            if fact.fact_ir[i] == 0 {
                break;
            }

            if let Some(resolved_constant) = self.inner.get_index_of(&fact.fact_ir[i]) {
                resolved_fact[i] = resolved_constant;
            } else {
                return None
            };
        }

        Some(resolved_fact)
    }
    pub fn resolve_goal(&self, goal: Goal) -> Option<InternedConstantTerms> {
        let mut resolved_partial_fact = [0; 3];
        for i in 0..3usize {
            if goal.goal_ir[i] != 0 {
                if let Some(resolved_constant) = self.inner.get_index_of(&goal.goal_ir[i]) {
                    resolved_partial_fact[i] = resolved_constant;
                } else {
                    return None
                };
            }
        }

        Some(resolved_partial_fact)
    }
    pub fn resolve_atom(&self, atom: Atom) -> Option<InternedAtom> {
        let mut resolved_atom = [(false, 0); 3];
        for i in 0..3usize {
            if !atom.atom_ir[i].0 {
                if !atom.atom_ir[i].1 == 0 {
                    break;
                }

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

        Self { inner }
    }
}