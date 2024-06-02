use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use datalog_syntax::TypedValue;
use crate::interning::hash::{new_random_state, reproducible_hash_one};

pub enum Term<T: Hash> {
    Var(String),
    Const(T)
}

#[allow(non_snake_case)]
pub fn Var(name: &str) -> Term<String> {
    return Term::Var(name.to_string())
}

#[allow(non_snake_case)]
pub fn Const<T: Hash>(value: T) -> Term<T> {
    return Term::Const(value)
}

type TermData = Option<Box<dyn Any>>;
type TermIR = (bool, u64);

impl<T> From<&Term<T>> for TermIR where T: Hash {
    fn from(value: &Term<T>) -> Self {
        match value {
            Term::Var(name) => (true, reproducible_hash_one(name)),
            Term::Const(value) => (false, reproducible_hash_one(value))
        }
    }
}

impl<T: 'static> From<Term<T>> for TermData where T: Hash {
    fn from(value: Term<T>) -> Self {
        match value {
            Term::Const(value) => Some(Box::new(value)),
            _ => None
        }
    }
}

type AtomData = [TermData; 3];
type AtomIR = [(bool, u64); 3];

pub struct Atom { pub(crate) atom_ir: AtomIR, pub(crate) atom_data: AtomData, pub(crate) symbol: u64 }

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.atom_ir.eq(&other.atom_ir) && self.symbol == other.symbol
    }
}

impl Eq for Atom {}

impl Debug for Atom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.atom_ir.fmt(f)
    }
}

impl<T: 'static> From<(&str, (Term<T>,))> for Atom where T: Hash {
    fn from(value: (&str, (Term<T>,))) -> Self {
        let first = TermIR::from(&value.1.0);
        let first_data = TermData::from(value.1.0);

        return Self { atom_ir: [ first, (false, 0), (false, 0)], atom_data: [ first_data, None, None ], symbol: reproducible_hash_one(value.0) }
    }
}

impl<T: 'static, R: 'static> From<(&str, (Term<T>, Term<R>))> for Atom where T: Hash, R: Hash {
    fn from(value: (&str, (Term<T>, Term<R>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let first_data = TermData::from(value.1.0);

        let second = TermIR::from(&value.1.1);
        let second_data = TermData::from(value.1.1);

        return Self { atom_ir: [first, second, (false, 0)], atom_data: [ first_data, second_data, None ], symbol: reproducible_hash_one(value.0) }
    }
}

impl<T: 'static, R: 'static, S: 'static> From<(&str, (Term<T>, Term<R>, Term<S>))> for Atom where T: Hash, R: Hash, S: Hash {
    fn from(value: (&str, (Term<T>, Term<R>, Term<S>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let first_data = TermData::from(value.1.0);

        let second = TermIR::from(&value.1.1);
        let second_data = TermData::from(value.1.1);

        let third = TermIR::from(&value.1.2);
        let third_data = TermData::from(value.1.2);

        return Self { atom_ir: [first, second, third], atom_data: [ first_data, second_data, third_data ], symbol: reproducible_hash_one(value.0) }
    }
}

pub type RuleIdentifier = u64;

#[derive(PartialEq, Eq, Debug)]
pub struct Rule { pub(crate) head: Atom, pub(crate) body: Vec<Atom>, pub(crate) id: RuleIdentifier }

impl<T, R> From<(T, Vec<R>)> for Rule where T: Into<Atom>, R: Into<Atom> {
    fn from(value: (T, Vec<R>)) -> Self {
        let head = value.0.into();
        let body: Vec<Atom> = value.1.into_iter().map(|body_atom| body_atom.into()).collect();

        let mut rs = new_random_state();
        head.atom_ir.hash(&mut rs);

        for body_atom in &body {
            body_atom.atom_ir.hash(&mut rs);
        }

        Self { head, body, id: rs.finish() }
    }
}

struct PositiveDatalogTerm (datalog_syntax::Term);

impl PositiveDatalogTerm {
    pub fn to_datalog_syntax_term(self) -> datalog_syntax::Term {
        self.0
    }
}

impl From<PositiveDatalogTerm> for TermIR {
    fn from(value: PositiveDatalogTerm) -> Self {
        match value.to_datalog_syntax_term() {
            datalog_syntax::Term::Variable(name) => {
                (&Var(name.as_str())).into()
            }
            datalog_syntax::Term::Constant(typed_value) => {
                match typed_value {
                    TypedValue::Str(inner) => { (&Const(inner)).into() }
                    TypedValue::Int(inner) => { (&Const(inner)).into()}
                    TypedValue::Bool(inner) => { (&Const(inner)).into() }
                }
            }
        }
    }
}

impl From<PositiveDatalogTerm> for TermData {
    fn from(value: PositiveDatalogTerm) -> Self {
        match value.to_datalog_syntax_term() {
            datalog_syntax::Term::Variable(_name) => {
                None
            }
            datalog_syntax::Term::Constant(typed_value) => {
                match typed_value {
                    TypedValue::Str(inner) => { Some(Box::new(inner)) }
                    TypedValue::Int(inner) => { Some(Box::new(inner)) }
                    TypedValue::Bool(inner) => { Some(Box::new(inner)) }
                }
            }
        }
    }
}

impl From<datalog_syntax::Rule> for Rule {
    fn from(value: datalog_syntax::Rule) -> Self {
        let head_symbol = value.head.symbol.as_str();
        let mut head_term_ir: AtomIR = [(false, 0); 3];
        let mut head_term_data: AtomData = Default::default();

        value
            .head
            .terms
            .into_iter()
            .map(|term| (TermIR::from(PositiveDatalogTerm(term.clone())), TermData::from(PositiveDatalogTerm(term))))
            .enumerate()
            .for_each(|(idx, (term_ir, term_data))| {
                head_term_ir[idx] = term_ir;
                head_term_data[idx] = term_data;
            });

        let head = Atom {
            atom_ir: head_term_ir,
            atom_data: head_term_data,
            symbol: reproducible_hash_one(head_symbol),
        };

        let body = value
            .body
            .into_iter()
            .map(|atom| {
                let current_atom_symbol = atom.symbol.as_str();
                let mut current_atom_term_ir: AtomIR = [(false, 0); 3];
                let mut current_atom_term_data: AtomData = Default::default();

                atom
                    .terms
                    .into_iter()
                    .map(|term| (TermIR::from(PositiveDatalogTerm(term.clone())), TermData::from(PositiveDatalogTerm(term))))
                    .enumerate()
                    .for_each(|(idx, (term_ir, term_data))| {
                        current_atom_term_ir[idx] = term_ir;
                        current_atom_term_data[idx] = term_data;
                    });
                
                Atom {
                    atom_ir: current_atom_term_ir,
                    atom_data: current_atom_term_data,
                    symbol: reproducible_hash_one(current_atom_symbol),
                }
            })
            .collect();

        Rule::from((head, body))
    }
}