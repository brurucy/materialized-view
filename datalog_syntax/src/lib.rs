use std::fmt::{Debug, Formatter};

#[derive(Eq, Ord, PartialEq, PartialOrd, Clone, Hash)]
pub enum TypedValue {
    Str(String),
    Int(usize),
    Bool(bool),
}

impl Debug for TypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypedValue::Str(x) => x.fmt(f),
            TypedValue::Int(x) => x.fmt(f),
            TypedValue::Bool(x) => x.fmt(f),
        }
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> Self {
        TypedValue::Str(value)
    }
}

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        TypedValue::Str(value.to_string())
    }
}

impl From<usize> for TypedValue {
    fn from(value: usize) -> Self {
        TypedValue::Int(value)
    }
}

impl From<bool> for TypedValue {
    fn from(value: bool) -> Self {
        TypedValue::Bool(value)
    }
}

pub type Variable = String;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash)]
pub enum Term {
    Variable(String),
    Constant(TypedValue),
}

impl Debug for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Variable(x) => x.fmt(f),
            Term::Constant(x) => x.fmt(f),
        }
    }
}

pub type AnonymousGroundAtom = Vec<TypedValue>;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash)]
pub struct Atom {
    pub terms: Vec<Term>,
    pub symbol: String,
}

impl Debug for Atom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", &self.symbol)?;

        for (index, term) in self.terms.iter().enumerate() {
            write!(f, "{:?}", term)?;
            if index < self.terms.len() - 1 {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash)]
pub struct Rule {
    pub head: Atom,
    pub body: Vec<Atom>,
}

impl Debug for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.head)?;
        write!(f, " <- [")?;
        for (index, atom) in self.body.iter().enumerate() {
            write!(f, "{:?}", atom)?;
            if index < self.body.len() - 1 {
                write!(f, ", ")?;
            }
        }

        write!(f, "]")
    }
}

pub type Program = Vec<Rule>;