use datalog_syntax::{AnonymousGroundAtom, Atom, Rule, Term, TypedValue};
use lasso::{Key, Rodeo};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{Debug};

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

pub fn reliably_intern_atom(atom: &Atom, interner: &mut Rodeo) -> InternedAtom {
    let head_symbol = interner.get_or_intern(atom.symbol.clone());
    let terms = atom
        .terms
        .iter()
        .map(|term| match term {
            Term::Variable(name) => {
                InternedTerm::Variable(interner.get_or_intern(name).into_usize())
            }
            Term::Constant(inner) => InternedTerm::Constant(inner.clone()),
        })
        .collect();

    return InternedAtom {
        terms: terms,
        symbol: head_symbol.into_usize(),
    };
}

pub fn reliably_intern_rule(rule: Rule, interner: &mut Rodeo) -> InternedRule {
    InternedRule {
        head: reliably_intern_atom(&rule.head, interner),
        body: rule
            .body
            .iter()
            .map(|atom| reliably_intern_atom(atom, interner))
            .collect(),
    }
}

pub type Domain = usize;
pub type Substitution = (Domain, TypedValue);

#[derive(
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    Default,
    Archive,
    Serialize,
    Deserialize,
    SizeOf,
)]
pub struct Rewrite {
    pub inner: Vec<Substitution>,
}

impl Rewrite {
    pub fn get(&self, key: Domain) -> Option<&TypedValue> {
        for sub in &self.inner {
            if sub.0 == key {
                return Some(&sub.1);
            }
        }

        None
    }
    pub fn insert(&mut self, value: Substitution) {
        if self.get(value.0).is_none() {
            self.inner.push(value)
        }
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn extend(&mut self, other: Self) {
        other.inner.into_iter().for_each(|sub| {
            self.insert(sub);
        })
    }
    pub fn apply(&self, atom: &Vec<InternedTerm>) -> Vec<InternedTerm> {
        return atom
            .iter()
            .map(|term| {
                if let InternedTerm::Variable(identifier) = term {
                    if let Some(constant) = self.get(*identifier) {
                        return InternedTerm::Constant(constant.clone());
                    }
                }

                term.clone()
            })
            .collect();
    }
    pub fn remove(&mut self, key: Domain) -> TypedValue {
        let position = self.inner.iter().position(|value| value.0 == key).unwrap();

        return self.inner.swap_remove(position).1;
    }
    pub fn ground(&self, atom: &Vec<InternedTerm>) -> AnonymousGroundAtom {
        atom.iter()
            .map(|term| {
                return match term {
                    InternedTerm::Variable(inner) => self.get(*inner).unwrap(),
                    InternedTerm::Constant(inner) => inner,
                }.clone();
            })
            .collect()
    }
}