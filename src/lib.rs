mod engine;
pub use engine::datalog::*;
pub use builders::goal::ANY_VALUE;
pub use ::datalog_syntax::*;
pub use ::datalog_syntax_macros::{rule, program};
pub use builders::rule::{Const, Var};
mod rewriting;
mod interning;
mod builders;
