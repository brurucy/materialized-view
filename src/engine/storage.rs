use indexmap::{IndexMap, IndexSet};
use crate::rewriting::atom::{EncodedFact};

pub type RelationIdentifier = u64;
pub type FactStorage = IndexSet<EncodedFact, identity_hash::BuildIdentityHasher<EncodedFact>>;
#[derive(Default)]
pub struct StorageLayer {
    pub(crate) inner: IndexMap<RelationIdentifier, FactStorage>,
}

impl StorageLayer {
    pub fn get_relation(&self, relation_identifier: &RelationIdentifier) -> &FactStorage {
        return self.inner.get(relation_identifier).unwrap()
    }
    pub fn push(&mut self, relation_identifier: &RelationIdentifier, fact: EncodedFact) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_identifier) {
            return relation.insert(fact);
        }

        let mut fresh_fact_storage = FactStorage::default();
        fresh_fact_storage.insert(fact);

        self.inner.insert(*relation_identifier, fresh_fact_storage);

        true
    }
    pub fn remove(&mut self, relation_identifier: &RelationIdentifier, fact: &EncodedFact) -> bool {
        if let Some(relation) = self.inner.get_mut(relation_identifier) {
            return relation.remove(fact);
        }

        false
    }
    pub fn contains(&self, relation_identifier: &RelationIdentifier, fact: &EncodedFact) -> bool {
        if let Some(relation) = self.inner.get(relation_identifier) {
            return relation.contains(fact);
        }

        false
    }
    pub fn len(&self) -> usize {
        self.inner.iter().map(|(_, fact_storage)| fact_storage.len()).sum()
    }
}
