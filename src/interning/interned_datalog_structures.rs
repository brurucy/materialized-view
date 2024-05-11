use datalog_syntax::{Atom, Rule, Term, TypedValue};
use lasso::{Key, Rodeo};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{Debug};
use crate::engine::storage::RelationStorage;

#[derive(
    Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Debug, Archive, Serialize, Deserialize, SizeOf,
)]
pub enum InternedTerm {
    Variable(usize),
    Constant(TypedValue),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Archive, Serialize, Deserialize, SizeOf)]
pub struct InternedAtom {
    pub terms: Vec<InternedTerm>,
    pub symbol: usize,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Archive, Serialize, Deserialize, SizeOf)]
pub struct InternedRule {
    pub head: InternedAtom,
    pub body: Vec<InternedAtom>,
}

pub fn intern_atom(atom: &Atom, variable_interner: &mut Rodeo, relation_storage: &RelationStorage) -> InternedAtom {
    let symbol = relation_storage.inner.get_index_of(&atom.symbol).unwrap();
    let terms = atom
        .terms
        .iter()
        .map(|term| match term {
            Term::Variable(name) => {
                InternedTerm::Variable((variable_interner.get_or_intern(name).into_usize()) + 1)
            }
            Term::Constant(inner) => InternedTerm::Constant(inner + 1),
        })
        .collect();

    return InternedAtom {
        terms,
        symbol,
    };
}

pub fn intern_rule(rule: Rule, interner: &mut Rodeo, relation_storage: &RelationStorage) -> InternedRule {
    InternedRule {
        head: intern_atom(&rule.head, interner, relation_storage),
        body: rule
            .body
            .iter()
            .map(|atom| intern_atom(atom, interner, relation_storage))
            .collect(),
    }
}

pub type RuleLocalVariableIdentifier = usize;
pub type Substitution = (RuleLocalVariableIdentifier, TypedValue);