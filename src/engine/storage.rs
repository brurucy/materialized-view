use indexmap::{IndexMap, IndexSet};
pub type RelationIdentifier = u64;
pub type InternedConstantTerms = [usize; 3];
pub type FactStorage = IndexSet<InternedConstantTerms>;
#[derive(Default)]
pub struct StorageLayer {
    pub(crate) inner: IndexMap<RelationIdentifier, FactStorage>,
}

impl StorageLayer {
    pub fn get_relation(&self, relation_identifier: &RelationIdentifier) -> &FactStorage {
        return self.inner.get(relation_identifier).unwrap()
    }
    pub fn push(&mut self, relation_identifier: &RelationIdentifier, fact: InternedConstantTerms) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_identifier) {
            return relation.insert(fact);
        }

        let mut fresh_fact_storage = FactStorage::default();
        fresh_fact_storage.insert(fact);

        self.inner
            .insert(*relation_identifier, fresh_fact_storage);

        true
    }
    pub fn remove(&mut self, relation_identifier: &RelationIdentifier, fact: &InternedConstantTerms) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_identifier) {
            return relation.remove(fact);
        }

        false
    }
    pub fn contains(&self, relation_identifier: &RelationIdentifier, fact: &InternedConstantTerms) -> bool {
        if let Some(relation) = self.inner.get(relation_identifier) {
            return relation.contains(fact);
        }

        false
    }
}
