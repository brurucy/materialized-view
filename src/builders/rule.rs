use std::hash::{BuildHasher, Hash, Hasher};
use ahash::RandomState;

pub enum Term<'a, T: Hash> {
    Var(&'a str),
    Const(T)
}

pub fn Var<'a>(name: &'a str) -> Term<&'a str> {
    return Term::Var(name)
}

pub fn Const<'a, T: Hash>(value: T) -> Term<'a, T> {
    return Term::Const(value)
}

type TermIR = (bool, u64);

impl<'a, T> From<&Term<'a, T>> for TermIR where T: Hash {
    fn from(value: &Term<'a, T>) -> Self {
        let rs = RandomState::new();

        match value {
            Term::Var(name) => (true, rs.hash_one(&name)),
            Term::Const(value) => (false, rs.hash_one(&value))
        }
    }
}

type AtomIR = [(bool, u64); 3];

pub struct Atom<'a> { pub(crate) atom_ir: AtomIR, pub(crate) symbol: &'a str }

impl<'a, T> From<(&'a str, (Term<'a, T>,))> for Atom<'a> where T: Hash {
    fn from(value: (&'a str, (Term<'a, T>,))) -> Self {
        let first = TermIR::from(&value.1.0);

        return Self { atom_ir: [ first, (false, 0), (false, 0)], symbol: value.0 }
    }
}

impl<'a, T, R> From<(&'a str, (Term<'a, T>, Term<'a, R>))> for Atom<'a> where T: Hash, R: Hash {
    fn from(value: (&'a str, (Term<'a, T>, Term<'a, R>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let second = TermIR::from(&value.1.1);

        return Self { atom_ir: [first, second, (false, 0)], symbol: "a" }
    }
}

impl<'a, T, R, S> From<(&'a str, (Term<'a, T>, Term<'a, R>, Term<'a, S>))> for Atom<'a> where T: Hash, R: Hash, S: Hash {
    fn from(value: (&'a str, (Term<'a, T>, Term<'a, R>, Term<'a, S>))) -> Self {
        let first = TermIR::from(&value.1.0);
        let second = TermIR::from(&value.1.1);
        let third = TermIR::from(&value.1.2);

        return Self { atom_ir: [first, second, third], symbol: value.0 }
    }
}

pub struct Rule<'a> { pub(crate) head: Atom<'a>, pub(crate) body: Vec<Atom<'a>>, pub(crate) id: u64 }

impl<'a> From<(Atom<'a>, Vec<Atom<'a>>)> for Rule<'a> {
    fn from(value: (Atom<'a>, Vec<Atom<'a>>)) -> Self {
        let mut rs = RandomState::new().build_hasher();
        value.0.atom_ir.hash(&mut rs);

        for body_atom in &value.1 {
            body_atom.atom_ir.hash(&mut rs);
        }

        Self { head: value.0, body: value.1, id: rs.finish() }
    }
}

mod tests {
    use crate::builders::rule::{Rule, Var};

    #[test]
    fn test_rule_builder() {
        let head = ("ancestor", (Var("?x"), Var("?z")));
        let body_atom_1 = ("parent", (Var("?x"), Var("?y")));
        let body_atom_2 = ("ancestor", (Var("?y"), Var("?z")));
        // ancestor(?x, ?z) <- parent(?x, ?y), ancestor(?y, ?z)
        let rule = Rule::from((head.into(), vec![body_atom_1.into(), body_atom_2.into()]));
    }
}