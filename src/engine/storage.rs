use dashmap::{DashMap, Map};
use datalog_syntax::AnonymousGroundAtom;
use indexmap::IndexSet;

pub type FactStorage = IndexSet<AnonymousGroundAtom, ahash::RandomState>;
#[derive(Default)]
pub struct RelationStorage {
    pub(crate) inner: DashMap<String, FactStorage>,
}

impl RelationStorage {
    pub fn get_relation(&self, relation_symbol: &str) -> &FactStorage {
        let shard = self.inner.determine_map(relation_symbol);
        let shard_map = unsafe { self.inner._get_read_shard(shard) };

        return shard_map.get(relation_symbol).unwrap().get();
    }
    pub fn insert(&mut self, relation_symbol: &str, ground_atom: AnonymousGroundAtom) -> bool {
        if let Some(mut relation) = self.inner.get_mut(relation_symbol) {
            return relation.insert(ground_atom);
        }

        let mut fresh_fact_storage = FactStorage::default();
        fresh_fact_storage.insert(ground_atom);

        self.inner
            .insert(relation_symbol.to_string(), fresh_fact_storage);

        true
    }
    pub fn remove(&mut self, relation_symbol: &str, ground_atom: &AnonymousGroundAtom) -> bool {
        if let Some(mut relation) = self.inner.get_mut(relation_symbol) {
            return relation.remove(ground_atom);
        }

        false
    }
    pub fn contains(&self, relation_symbol: &str, ground_atom: &AnonymousGroundAtom) -> bool {
        if let Some(relation) = self.inner.get(relation_symbol) {
            return relation.contains(ground_atom);
        }

        false
    }
}
