use indexmap::{IndexMap, IndexSet};
pub type InternedFact = [usize; 3];
pub type FactStorage = IndexSet<InternedFact>;
#[derive(Default)]
pub struct StorageLayer {
    pub(crate) inner: IndexMap<String, FactStorage>,
}

impl StorageLayer {
    pub fn get_relation(&self, relation_symbol: &str) -> &FactStorage {
        return self.inner.get(relation_symbol).unwrap()
    }
    pub fn push(&mut self, relation_symbol: &str, fact: InternedFact) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_symbol) {
            return relation.insert(fact);
        }

        let mut fresh_fact_storage = FactStorage::default();
        fresh_fact_storage.insert(fact);

        self.inner
            .insert(relation_symbol.to_string(), fresh_fact_storage);

        true
    }
    pub fn remove(&mut self, relation_symbol: &str, fact: &InternedFact) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_symbol) {
            return relation.remove(fact);
        }

        false
    }
    pub fn contains(&self, relation_symbol: &str, fact: &InternedFact) -> bool {
        if let Some(relation) = self.inner.get(relation_symbol) {
            return relation.contains(fact);
        }

        false
    }
}
