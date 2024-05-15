use indexmap::{IndexSet};
use crate::builders::fact::Fact;
use crate::engine::storage::InternedFact;

#[derive(Default)]
pub struct InternmentLayer {
    inner: IndexSet<u64>
}

impl InternmentLayer {
    pub fn push(&mut self, hash: u64) -> usize {
        // 0 is a reserved value. It is used to denote emptiness.
        self.inner.insert_full(hash).0 + 1
    }
    pub fn intern_fact(&mut self, fact: Fact) -> InternedFact {
        return [ self.push(fact.fact_ir[0]), self.push(fact.fact_ir[1]), self.push(fact.fact_ir[2]) ]
    }
    pub fn resolve_fact_constants(&self, fact: Fact) -> Option<InternedFact> {
        let mut resolved_fact = [0; 3];
        for i in 0..3usize {
            if let Some(resolved_constant) = self.inner.get_index_of(&fact.fact_ir[i]) {
                resolved_fact[i] = resolved_constant;
            } else {
                return None
            };
        }

        return Some(resolved_fact)
    }
}