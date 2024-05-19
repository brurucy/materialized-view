use ahash::RandomState;
use datalog_syntax::Program;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::builders::fact::Fact;
use crate::builders::goal::{Goal, pattern_match};
use crate::builders::rule::Rule;
use crate::engine::compute::ComputeLayer;
use crate::interning::hash::new_random_state;
use crate::interning::herbrand_universe::InternmentLayer;

pub struct MaterializedDatalogView {
    compute_layer: ComputeLayer,
    internment_layer: InternmentLayer,
    storage_layer: StorageLayer,
    rs: RandomState,
    safe: bool,
}

pub type RelationSymbol<'a> = &'a str;

impl MaterializedDatalogView {
    pub fn push_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        if let Some(fact_storage) = self.storage_layer.inner.get_mut(&hashed_relation_symbol) {
            let interned_fact = self.internment_layer.intern_fact(fact.into());

            self.compute_layer.send_fact(hashed_relation_symbol, &interned_fact);
            self.safe = false;

            return fact_storage.insert(interned_fact)
        }

        false
    }
    pub fn retract_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        if let Some(fact_storage) = self.storage_layer.inner.get_mut(&hashed_relation_symbol) {
            let interned_fact = self.internment_layer.resolve_fact(fact.into()).unwrap();

            self.compute_layer.retract_fact(hashed_relation_symbol, &interned_fact);
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
        self.safe = false;
    }
    pub fn retract_rule(&mut self, rule: impl Into<Rule>) {
        let rule = rule.into();

        self.compute_layer.retract_rule(&self.internment_layer.resolve_rule(rule).unwrap());
        self.safe = false;
    }
    pub fn contains(
        &self,
        relation_symbol: RelationSymbol,
        fact: impl Into<Fact>,
    ) -> Result<bool, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        if let Some(interned_fact) = self.internment_layer.resolve_fact(fact.into()) {
            return Ok(self.storage_layer.contains(&hashed_relation_symbol, &interned_fact))
        }

        Ok(false)
    }
    pub fn query_unary<'a, T: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>
    ) -> Result<impl Iterator<Item=(&T, )>, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = self.storage_layer.get_relation(&hashed_relation_symbol);

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|interned_constant_terms| {
                let resolved_interned_constant_term = self.internment_layer.resolve_hash::<T>(interned_constant_terms[0] as u64).unwrap();

                (resolved_interned_constant_term,)
            }))
    }
    pub fn query_binary<'a, T: 'static, R: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>
    ) -> Result<impl Iterator<Item=(&T, &R)>, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = self.storage_layer.get_relation(&hashed_relation_symbol);

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|interned_constant_terms| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_hash::<T>(interned_constant_terms[0] as u64).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_hash::<R>(interned_constant_terms[1] as u64).unwrap();

                (resolved_interned_constant_term_one, resolved_interned_constant_term_two)
            }))
    }
    pub fn query_ternary<'a, T: 'static, R: 'static, S: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>
    ) -> Result<impl Iterator<Item=(&T, &R, &S)>, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = self.storage_layer.get_relation(&hashed_relation_symbol);

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|interned_constant_terms| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_hash::<T>(interned_constant_terms[0] as u64).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_hash::<R>(interned_constant_terms[1] as u64).unwrap();
                let resolved_interned_constant_term_three = self.internment_layer.resolve_hash::<S>(interned_constant_terms[2] as u64).unwrap();

                (resolved_interned_constant_term_one, resolved_interned_constant_term_two, resolved_interned_constant_term_three)
            }))
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
        let mut materialized_datalog_view = Self { compute_layer, internment_layer: herbrand_universe, storage_layer, rs: new_random_state(), safe: true };

        program.inner.into_iter().for_each(|rule| {
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
    use crate::builders::goal::ANY_VALUE;

    type NodeIndex = usize;
    type Edge = (NodeIndex, NodeIndex);

    #[test]
    fn integration_test_push_fact() {
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
        .for_each(|edge: Edge| {
            materialized_datalog_view.push_fact("e", edge);
        });

        assert_eq!(materialized_datalog_view.len(), 3);
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 9);

        let actual_all: HashSet<Edge> =
            materialized_datalog_view
                .query_binary("tc", (ANY_VALUE, ANY_VALUE))
                .unwrap()
                .map(|(x, y)| (*x, *y))
                .collect();
        let expected_all: HashSet<Edge> = vec![
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

        let actual_all_from_a: HashSet<Edge> =
            materialized_datalog_view.query_binary("tc", (Some(1), ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
        let expected_all_from_a: HashSet<Edge> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_from_a, actual_all_from_a);

        expected_all.iter().for_each(|fact| {
            assert!(materialized_datalog_view.contains("tc", *fact).unwrap());
        });

        expected_all_from_a.iter().for_each(|fact| {
            assert!(materialized_datalog_view.contains("tc", *fact).unwrap());
        });

        materialized_datalog_view.push_fact("e", (4, 5));
        assert!(!materialized_datalog_view.safe());
        // Update
        materialized_datalog_view.poll();
        assert!(materialized_datalog_view.safe());

        let actual_all_after_update: HashSet<Edge> =
            materialized_datalog_view
                .query_binary("tc", (ANY_VALUE, ANY_VALUE))
                .unwrap()
                .map(|(x, y)| (*x, *y))
                .collect();
        let expected_all_after_update: HashSet<Edge> = vec![
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

        let actual_all_from_a_after_update: HashSet<Edge> = materialized_datalog_view
            .query_binary("tc", (Some(1), ANY_VALUE))
            .unwrap()
            .map(|(x, y)| (*x, *y))
            .collect();
        let expected_all_from_a_after_update: HashSet<Edge> = vec![
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
    fn integration_test_retract_fact() {
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

        let actual_all: HashSet<Edge> =
            runtime.query_binary("tc", (ANY_VALUE, ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
        let expected_all: HashSet<Edge> = vec![
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

        let actual_all_from_a: HashSet<Edge> = runtime
            .query_binary("tc", (Some(1), ANY_VALUE))
            .unwrap()
            .map(|(x, y)| (*x, *y))
            .collect();
        let expected_all_from_a: HashSet<Edge> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_from_a, actual_all_from_a);

        // Update
        runtime.retract_fact("e", (4, 5));
        assert!(!runtime.safe());
        runtime.poll();
        assert!(runtime.safe());

        let actual_all_after_update: HashSet<Edge> =
            runtime.query_binary("tc", (ANY_VALUE, ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
        let expected_all_after_update: HashSet<Edge> = vec![
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

        let actual_all_from_a_after_update: HashSet<Edge> = runtime
            .query_binary("tc", (Some(1), ANY_VALUE))
            .unwrap()
            .map(|(x, y)| (*x, *y))
            .collect();
        let expected_all_from_a_after_update: HashSet<Edge> = vec![
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
