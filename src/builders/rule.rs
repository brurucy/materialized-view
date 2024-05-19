use std::any::Any;
use std::hash::{BuildHasher, Hash, Hasher};
use datalog_syntax::TypedValue;
use crate::interning::hash::new_random_state;

pub enum Term<T: Hash> {
    Var(String),
    Const(T)
}

pub fn Var(name: &str) -> Term<String> {
    return Term::Var(name.to_string())
}

pub fn Const<T: Hash>(value: T) -> Term<T> {
    return Term::Const(value)
}

type TermData = Option<Box<dyn Any>>;
type TermIR = (bool, u64);

impl<T> From<&Term<T>> for TermIR where T: Hash {
    fn from(value: &Term<T>) -> Self {
        let rs = new_random_state();

        match value {
            Term::Var(name) => (true, rs.hash_one(name.as_str())),
            Term::Const(value) => (false, rs.hash_one(value))
        }
    }
}

type AtomData = [TermData; 3];
type AtomIR = [(bool, u64); 3];

pub struct Atom { pub(crate) atom_ir: AtomIR, pub(crate) atom_data: AtomData, pub(crate) symbol: u64 }

impl<T: 'static> From<(&str, (Term<T>,))> for Atom where T: Hash {
    fn from(value: (&str, (Term<T>,))) -> Self {
        let first = TermIR::from(&value.1.0);

        return Self { atom_ir: [ first, (false, 0), (false, 0)], atom_data: [ Some(Box::new(value.1.0)), None, None ], symbol: new_random_state().hash_one(value.0) }
    }
}

impl<T: 'static, R: 'static> From<(&str, (Term<T>, Term<R>))> for Atom where T: Hash, R: Hash {
    fn from(value: (&str, (Term<T>, Term<R>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let second = TermIR::from(&value.1.1);

        return Self { atom_ir: [first, second, (false, 0)], atom_data: [ Some(Box::new(value.1.0)), Some(Box::new(value.1.1)), None ], symbol: new_random_state().hash_one(value.0) }
    }
}

impl<T: 'static, R: 'static, S: 'static> From<(&str, (Term<T>, Term<R>, Term<S>))> for Atom where T: Hash, R: Hash, S: Hash {
    fn from(value: (&str, (Term<T>, Term<R>, Term<S>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let second = TermIR::from(&value.1.1);
        let third = TermIR::from(&value.1.2);

        return Self { atom_ir: [first, second, third], atom_data: [ Some(Box::new(value.1.0)), Some(Box::new(value.1.1)), Some(Box::new(value.1.2)) ], symbol: new_random_state().hash_one(value.0) }
    }
}

pub type RuleIdentifier = u64;

pub struct Rule { pub(crate) head: Atom, pub(crate) body: Vec<Atom>, pub(crate) id: RuleIdentifier }

impl From<(Atom, Vec<Atom>)> for Rule {
    fn from(value: (Atom, Vec<Atom>)) -> Self {
        let mut rs = new_random_state().build_hasher();
        value.0.atom_ir.hash(&mut rs);

        for body_atom in &value.1 {
            body_atom.atom_ir.hash(&mut rs);
        }

        Self { head: value.0, body: value.1, id: rs.finish() }
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

impl<'a> From<datalog_syntax::Rule> for Rule {
    fn from(value: datalog_syntax::Rule) -> Self {
        let rs = new_random_state();

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
            symbol: rs.hash_one(head_symbol),
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
                    symbol: rs.hash_one(current_atom_symbol),
                }
            })
            .collect();

        Rule::from((head, body))
    }
}