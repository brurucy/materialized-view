use ahash::RandomState;
use datalog_syntax::Program;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::builders::fact::Fact;
use crate::builders::rule::Rule;
use crate::engine::compute::ComputeLayer;
use crate::interning::herbrand_universe::InternmentLayer;

pub struct MaterializedDatalogView {
    compute_layer: ComputeLayer,
    internment_layer: InternmentLayer,
    storage_layer: StorageLayer,
    // Translation layer?
    rs: RandomState,
    safe: bool,
}

impl MaterializedDatalogView {
    pub fn push_fact(&mut self, relation: &str, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation);
        if let Some(fact_storage) = self.storage_layer.inner.get_mut(&hashed_relation_symbol) {
            let interned_fact = self.internment_layer.intern_fact(fact.into());

            self.compute_layer.send_fact(hashed_relation_symbol, &interned_fact);
            self.safe = false;

            return fact_storage.insert(interned_fact)
        }

        false
    }
    pub fn retract_fact(&mut self, relation: &str, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation);
        if let Some(fact_storage) = self.storage_layer.inner.get_mut(&hashed_relation_symbol) {
            let interned_fact = self.internment_layer.intern_fact(fact.into());

            self.compute_layer.send_fact(hashed_relation_symbol, &interned_fact);
            self.safe = false;

            return fact_storage.remove(&interned_fact)
        }

        false
    }
    fn ensure_relation_exists(&mut self, relation_identifier: &RelationIdentifier) {
        self.storage_layer.inner.entry(*relation_identifier).or_default();
    }
    fn ensure_rule_relations_exist(&mut self, rule: &Rule) {
        self.ensure_relation_exists(&rule.head.symbol);

        rule.body.iter().for_each(|body_atom| {
            self.ensure_relation_exists(&body_atom.symbol);
        });
    }
    pub fn push_rule(&mut self, rule: impl Into<Rule>) {
        let rule = rule.into();
        self.ensure_rule_relations_exist(&rule);
        
        self.compute_layer.send_rule(self.internment_layer.intern_rule(rule));
    }
    pub fn retract_rule(&mut self, rule: impl Into<Rule>) {
        let rule = rule.into();

        self.compute_layer.retract_rule(&self.internment_layer.intern_rule(rule));
    }
    pub fn contains(
        &self,
        relation: &str,
        fact: impl Into<Fact>,
    ) -> Result<bool, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        let hashed_relation_symbol = self.rs.hash_one(&relation);
        if let Some(interned_fact) = self.internment_layer.resolve_fact(fact.into()) {
            return Ok(self.storage_layer.contains(&hashed_relation_symbol, &interned_fact))
        }

        Ok(false)
    }
    fn step(&mut self) {
        self.compute_layer.step();
    }
    fn consolidate(&mut self) {
        self.compute_layer.consolidate_into_storage_layer(&mut self.storage_layer)
    }
    pub fn poll(&mut self) {
        self.step();
        self.consolidate();
        self.safe = true;
    }
    pub fn new(program: Program) -> Self {
        let storage_layer: StorageLayer = Default::default();
        let compute_layer = ComputeLayer::new();
        let herbrand_universe = InternmentLayer::default();
        let mut materialized_datalog_view = Self { compute_layer, internment_layer: herbrand_universe, storage_layer, rs: RandomState::new(), safe: true };

        program.inner.into_iter().for_each(|rule| {
            let rule: Rule = rule.into();

            materialized_datalog_view.ensure_rule_relations_exist(&rule);
            materialized_datalog_view.push_rule(rule);
        });

        materialized_datalog_view
    }
    pub fn safe(&self) -> bool {
        self.safe
    }
    pub fn len(&self) -> usize {
        self.storage_layer.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::datalog::MaterializedDatalogView;
    use datalog_syntax_macros::program;
    use datalog_syntax::*;
    use std::collections::HashSet;
    use datalog_syntax::build_query;

    #[test]
    fn integration_test_insertions_only() {
        let tc_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)],
        };

        let mut materialized_datalog_view = MaterializedDatalogView::new(tc_program);
        vec![
            (1, 2),
            (2, 3),
            (3, 4),
        ]
        .into_iter()
        .for_each(|edge| {
            materialized_datalog_view.push_fact("e", edge);
        });

        materialized_datalog_view.poll();

        // This query reads as: "Get all in tc with any values in any positions"
        let all = build_query!(tc(_, _));
        // And this one as: "Get all in tc with the first term being a"
        // There also is a QueryBuilder, if you do not want to use a macro.
        let all_from_a = build_query!(tc(1, _));

        let actual_all: HashSet<AnonymousGroundAtom> =
            materialized_datalog_view.query(&all).unwrap().collect();
        let expected_all: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (2, 3),
            (3, 4),
            // Second iter
            (1, 3),
            (2, 4),
            // Third iter
            (1, 4),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all, actual_all);

        let actual_all_from_a: HashSet<AnonymousGroundAtom> =
            materialized_datalog_view.query(&all_from_a).unwrap().collect();
        let expected_all_from_a: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_from_a, actual_all_from_a);

        expected_all.iter().for_each(|fact| {
            assert!(materialized_datalog_view.contains("tc", fact).unwrap());
        });

        expected_all_from_a.iter().for_each(|fact| {
            assert!(materialized_datalog_view.contains("tc", fact).unwrap());
        });

        // Update
        materialized_datalog_view.push_fact("e", (4, 5));
        assert!(!materialized_datalog_view.safe());
        materialized_datalog_view.poll();
        assert!(materialized_datalog_view.safe());

        let actual_all_after_update: HashSet<AnonymousGroundAtom> =
            materialized_datalog_view.query(&all).unwrap().collect();
        let expected_all_after_update: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (2, 3),
            (3, 4),
            // Second iter
            (1, 3),
            (2, 4),
            // Third iter
            (1, 4),
            // Update
            (4, 5),
            (3, 5),
            (2, 5),
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_after_update, actual_all_after_update);

        let actual_all_from_a_after_update: HashSet<AnonymousGroundAtom> = materialized_datalog_view
            .query(&all_from_a)
            .unwrap()
            .into_iter()
            .collect();
        let expected_all_from_a_after_update: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            expected_all_from_a_after_update,
            actual_all_from_a_after_update
        );
    }
    #[test]
    fn integration_test_deletions() {
        // Queries. The explanation is in the test above
        let all = build_query!(tc(_, _));
        let all_from_a = build_query!(tc(1, _));

        let tc_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [tc(?x, ?y), tc(?y, ?z)],
        };

        let mut runtime = MaterializedDatalogView::new(tc_program);
        vec![
            (1, 2),
            // this extra atom will help with testing that rederivation works
            (1, 5),
            (2, 3),
            (3, 4),
            (4, 5),
        ]
        .into_iter()
        .for_each(|edge| {
            runtime.push_fact("e", edge);
        });

        runtime.poll();

        let actual_all: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().collect();
        let expected_all: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (1, 5),
            (2, 3),
            (3, 4),
            // Second iter
            (1, 3),
            (2, 4),
            // Third iter
            (1, 4),
            // Fourth iter
            (4, 5),
            (3, 5),
            (2, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all, actual_all);

        let actual_all_from_a: HashSet<_> = runtime
            .query(&all_from_a)
            .unwrap()
            .into_iter()
            .collect();
        let expected_all_from_a: HashSet<_> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_from_a, actual_all_from_a);

        // Update
        // Point removals are a bit annoying, since they incur creating a query.
        runtime.retract_fact("e", (4, 5));
        assert!(!runtime.safe());
        runtime.poll();
        assert!(runtime.safe());

        let actual_all_after_update: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().collect();
        let expected_all_after_update: HashSet<AnonymousGroundAtom> = vec![
            (1, 2),
            (2, 3),
            (3, 4),
            // Second iter
            (1, 3),
            (2, 4),
            // Third iter
            (1, 4),
            // This remains
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_after_update, actual_all_after_update);

        let actual_all_from_a_after_update: HashSet<_> = runtime
            .query(&all_from_a)
            .unwrap()
            .into_iter()
            .collect();
        let expected_all_from_a_after_update: HashSet<_> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            expected_all_from_a_after_update,
            actual_all_from_a_after_update
        );
    }
}
