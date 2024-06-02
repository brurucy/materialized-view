use std::any::Any;
use identity_hash::BuildIdentityHasher;
use indexmap::{IndexMap, IndexSet};
use crate::builders::fact::Fact;
use crate::builders::goal::Goal;
use crate::builders::rule::{Atom, Rule};
use crate::engine::storage::{RelationIdentifier};
use crate::rewriting::atom::{encode_goal, EncodedGoal};

const CONSTANT_MAXIMUM_LEN: usize = (1 << 24) - 1;
const VARIABLE_MAXIMUM_LEN: usize = (1 << 8) - 1;

type ConstantInterner = IndexMap<u64, Box<dyn Any>, BuildIdentityHasher<u64>>;
type VariableInterner = IndexSet<u64, BuildIdentityHasher<u64>>;
#[derive(Default)]
pub struct InternmentLayer {
    constant_interner: ConstantInterner,
    variable_interner: VariableInterner,
}

pub type InternedTerm = usize;
pub type InternedTerms = [(bool, InternedTerm); 3];
pub type InternedAtom = (RelationIdentifier, InternedTerms);
pub type RuleIdentifier = u64;
pub type InternedRule = (RuleIdentifier, InternedAtom, Vec<InternedAtom>);
pub type InternedConstantTerms = [usize; 3];

impl InternmentLayer {
    pub fn resolve_interned_constant<T: 'static>(&self, interned_constant: usize) -> Option<&T> {
        if let Some(actual_value) = self.constant_interner.get_index(interned_constant - 1) {
            return Some(actual_value.1.downcast_ref::<T>().unwrap())
        }

        None
    }
    pub fn push_constant(&mut self, hash: u64, data: Box<dyn Any>) -> usize {
        if self.constant_interner.len() == CONSTANT_MAXIMUM_LEN {
            panic!("There can not be more than {} unique constant values", CONSTANT_MAXIMUM_LEN)
        }

        self.constant_interner.insert_full(hash, data).0 + 1
    }
    pub fn push_variable(&mut self, hash: u64) -> usize {
        if self.constant_interner.len() == VARIABLE_MAXIMUM_LEN {
            panic!("There can not be more than {} unique constant values", VARIABLE_MAXIMUM_LEN)
        }

        self.variable_interner.insert_full(hash).0 + 1
    }
    pub fn intern_fact(&mut self, fact: Fact) -> InternedConstantTerms {
        let mut interned_constant_terms = [0; 3];
        let mut fact = fact;

        let first_key = *fact.fact_ir.get(0).unwrap();
        if let Some(first_value) = fact.fact_data[0].take() {
            interned_constant_terms[0] = self.push_constant(first_key, first_value);
        }

        let second_key = *fact.fact_ir.get(1).unwrap();
        if let Some(second_value) = fact.fact_data[1].take() {
            interned_constant_terms[1] = self.push_constant(second_key, second_value);
        }
        
        let third_key = *fact.fact_ir.get(2).unwrap();
        if let Some(third_value) = fact.fact_data[2].take() {
            interned_constant_terms[2] = self.push_constant(third_key, third_value);
        }

        interned_constant_terms
    }
    pub fn intern_atom(&mut self, atom: Atom) -> InternedAtom {
        let mut interned_atom_ir = [ (false, 0); 3 ];
        let mut atom = atom;

        let first_key = atom.atom_ir[0];
        if first_key.1 != 0 {
            if !first_key.0 {
                interned_atom_ir[0] = (false, self.push_constant(first_key.1, atom.atom_data[0].take().unwrap()));
            } else {
                interned_atom_ir[0] = (true, self.push_variable(first_key.1))
            }
        }

        let second_key = atom.atom_ir[1];
        if second_key.1 != 0 {
            if !second_key.0 {
                interned_atom_ir[1] = (false, self.push_constant(second_key.1, atom.atom_data[1].take().unwrap()));
            } else {
                interned_atom_ir[1] = (true, self.push_variable(second_key.1));
            }
        }

        let third_key = atom.atom_ir[2];
        if third_key.1 != 0 {
            if !third_key.0 {
                interned_atom_ir[2] = (false, self.push_constant(third_key.1, atom.atom_data[2].take().unwrap()));
            } else {
                interned_atom_ir[2] = (true, self.push_variable(third_key.1));
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

            if let Some(resolved_constant) = self.constant_interner.get_index_of(&fact.fact_ir[i]) {
                resolved_fact[i] = resolved_constant + 1;
            } else {
                return None
            };
        }

        Some(resolved_fact)
    }
    pub fn resolve_goal(&self, goal: Goal) -> Option<EncodedGoal> {
        let mut resolved_partial_fact = [0; 3];
        for i in 0..3usize {
            if goal.goal_ir[i] != 0 {
                if let Some(resolved_constant) = self.constant_interner.get_index_of(&goal.goal_ir[i]) {
                    resolved_partial_fact[i] = resolved_constant + 1;
                } else {
                    return None
                };
            }
        }

        Some(encode_goal(&resolved_partial_fact))
    }
    pub fn resolve_atom(&self, atom: Atom) -> Option<InternedAtom> {
        let mut resolved_atom = [(false, 0); 3];
        for i in 0..3usize {
            if atom.atom_ir[i].1 != 0 {
                if !atom.atom_ir[i].0 {
                    if let Some(resolved_constant) = self.constant_interner.get_index_of(&atom.atom_ir[i].1) {
                        resolved_atom[i] = (false, resolved_constant + 1);
                    } else {
                        return None
                    }
                } else {
                    if let Some(resolved_variable) = self.variable_interner.get_index_of(&atom.atom_ir[i].1) {
                        resolved_atom[i] = (true, resolved_variable + 1);
                    } else {
                        return None
                    }
                }
            }
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
}