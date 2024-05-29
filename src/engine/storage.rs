use identity_hash::{BuildIdentityHasher};
use indexmap::{IndexMap, IndexSet};
use crate::engine::compute::Weight;
use crate::rewriting::atom::{EncodedFact};
type ConsolidatedFactStorage = IndexSet<EncodedFact, BuildIdentityHasher<EncodedFact>>;
type Frontier = Vec<(EncodedFact, Weight)>;
pub(crate) type RelationIdentifier = u64;
#[derive(Default)]
pub(crate) struct StorageLayer {
    pub(crate) inner: IndexMap<RelationIdentifier, (Frontier, ConsolidatedFactStorage)>,
}

impl StorageLayer {
    pub fn get_relations(&self, relation_identifier: &RelationIdentifier) -> &(Frontier, ConsolidatedFactStorage) {
        return self.inner.get(relation_identifier).unwrap()
    }
    pub fn update(&mut self, relation_identifier: &RelationIdentifier, fact: EncodedFact, weight: Weight) -> bool {
        if let Some((frontier, consolidated)) = self.inner.get_mut(relation_identifier) {
            frontier.push((fact, weight));

            return if weight > 0 {
                consolidated.insert(fact)
            } else {
                consolidated.remove(&fact)
            }
        }

        let mut fresh_fact_storages: (Frontier, ConsolidatedFactStorage) = Default::default();
        fresh_fact_storages.0.push((fact, weight));
        if weight > 0 {
            fresh_fact_storages.1.insert(fact);
        } else {
            fresh_fact_storages.1.remove(&fact);
        }

        self.inner.insert(*relation_identifier, fresh_fact_storages);

        true
    }
    pub fn contains(&self, relation_identifier: &RelationIdentifier, fact: &EncodedFact) -> bool {
        if let Some(relation) = self.inner.get(relation_identifier) {
            return relation.1.contains(fact);
        }

        false
    }
    pub fn len(&self) -> usize {
        self.inner.iter().map(|(_, fact_storage)| fact_storage.1.len()).sum()
    }
    pub fn move_frontier(&mut self) {
        for (_, (frontier, _)) in self.inner.iter_mut() {
            frontier.clear();
        }
    }
}
