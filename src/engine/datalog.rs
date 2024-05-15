use crate::engine::storage::StorageLayer;
use lasso::{Rodeo, Spur};
use crate::builders::fact::Fact;
use crate::builders::rule::Rule;
use crate::engine::compute::ComputeLayer;
use crate::interning::herbrand_universe::InternmentLayer;
use crate::interning::rule::intern_rule;

pub struct DyreRuntime {
    compute_layer: ComputeLayer,
    internment_layer: InternmentLayer,
    storage_layer: StorageLayer,
    // Translation layer?
    safe: bool,
}

impl DyreRuntime {
    pub fn push_fact(&mut self, relation: &str, fact: impl Into<Fact>) -> bool {
        if let Some(interned_symbol) = self.storage_layer.inner.get_index_of(relation) {
            let interned_fact = self.internment_layer.intern_fact(fact.into());

            self.compute_layer.send_fact(interned_symbol, &interned_fact);
            self.safe = false;

            return self.storage_layer.push(relation, interned_fact)
        }

        false
    }
    pub fn retract_fact(&mut self, relation: &str, fact: impl Into<Fact>) -> bool {
        if let Some(interned_symbol) = self.storage_layer.inner.get_index_of(relation) {
            let interned_fact = self.internment_layer.intern_fact(fact.into());

            self.compute_layer.retract_fact(interned_symbol, &interned_fact);
            self.safe = false;

            return self.storage_layer.remove(relation, &interned_fact)
        }

        false
    }
    fn ensure_relation_exists(&mut self, relation_symbol: &str) {
        self.storage_layer.inner.entry(relation_symbol.to_string()).or_default();
    }
    fn ensure_rule_relations_exist(&mut self, rule: &Rule) {
        self.ensure_relation_exists(&rule.head.symbol);

        rule.body.iter().for_each(|body_atom| {
            self.ensure_relation_exists(&body_atom.symbol);
        });
    }
    pub fn push_rule(&mut self, rule: Rule) {
        let mut variable_interner: Rodeo<Spur> = Rodeo::new();
        self.ensure_rule_relations_exist(&rule);

        let rule_id = rule.id;
        let interned_rule = intern_rule(rule, &mut variable_interner, &self.storage_layer);

        self.compute_layer.send_rule(rule_id, interned_rule);
    }
    pub fn retract_rule(&mut self, rule: &Rule) {
        let mut variable_interner: Rodeo<Spur> = Rodeo::new();
        let rule_id = rule.id;
        let interned_rule = intern_rule(rule.clone(), &mut variable_interner, &self.storage_layer);

        self.compute_layer.retract_rule(rule_id, interned_rule);
    }
    pub fn contains(
        &self,
        relation: &str,
        fact: impl Into<Fact>,
    ) -> Result<bool, String> {
        if !self.safe() {
            return Err("polling is needed to obtain correct results".to_string());
        }

        if let Some(interned_fact) = self.internment_layer.resolve_fact_constants(fact.into()) {
            return Ok(self.storage_layer.contains(relation, &interned_fact))
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
        let mut dyre_runtime = Self { compute_layer, internment_layer: herbrand_universe, storage_layer, safe: true };

        program.inner.iter().for_each(|rule| {
            dyre_runtime.ensure_rule_relations_exist(&rule);
            dyre_runtime.push_rule(rule.clone());
        });

        dyre_runtime
    }
    pub fn safe(&self) -> bool {
        self.safe
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_any_map() {
        use std::any::{Any, TypeId};

        let boxed: Box<dyn Any> = Box::new((0usize, 1u8, "a"));

        let actual_id = (&*boxed).type_id();
        let boxed_id = boxed.type_id();
        let derefed_box_vec: Vec<&(usize, u8, &str)> = [&boxed].into_iter().map(|x| boxed.downcast_ref().unwrap()).collect();

        assert_eq!(actual_id, TypeId::of::<(usize, u8, &str)>());
        assert_eq!(boxed_id, TypeId::of::<Box<dyn Any>>());
    }
}

/*#[cfg(test)]
mod tests {
    use crate::engine::datalog::DyreRuntime;
    use datalog_rule_macro::program;
    use datalog_syntax::*;
    use std::collections::HashSet;

    #[test]
    fn integration_test_insertions_only() {
        let tc_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)],
        };

        let mut runtime = DyreRuntime::new(tc_program);
        vec![
            vec![1, 2],
            vec![2, 3],
            vec![3, 4],
        ]
        .into_iter()
        .for_each(|edge| {
            runtime.push_fact("e", edge);
        });

        runtime.poll();

        // This query reads as: "Get all in tc with any values in any positions"
        let all = build_query!(tc(_, _));
        // And this one as: "Get all in tc with the first term being a"
        // There also is a QueryBuilder, if you do not want to use a macro.
        let all_from_a = build_query!(tc(1, _));

        let actual_all: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().collect();
        let expected_all: HashSet<AnonymousGroundAtom> = vec![
            vec![1, 2],
            vec![2, 3],
            vec![3, 4],
            // Second iter
            vec![1, 3],
            vec![2, 4],
            // Third iter
            vec![1, 4],
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all, actual_all);

        let actual_all_from_a: HashSet<AnonymousGroundAtom> =
            runtime.query(&all_from_a).unwrap().collect();
        let expected_all_from_a: HashSet<AnonymousGroundAtom> = vec![
            vec![1, 2],
            vec![1, 3],
            vec![1, 4],
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_from_a, actual_all_from_a);

        expected_all.iter().for_each(|fact| {
            assert!(runtime.contains("tc", fact).unwrap());
        });

        expected_all_from_a.iter().for_each(|fact| {
            assert!(runtime.contains("tc", fact).unwrap());
        });

        // Update
        runtime.push_fact("e", vec![4, 5]);
        assert!(!runtime.safe());
        runtime.poll();
        assert!(runtime.safe());

        let actual_all_after_update: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().collect();
        let expected_all_after_update: HashSet<AnonymousGroundAtom> = vec![
            vec![1, 2],
            vec![2, 3],
            vec![3, 4],
            // Second iter
            vec![1, 3],
            vec![2, 4],
            // Third iter
            vec![1, 4],
            // Update
            vec![4, 5],
            vec![3, 5],
            vec![2, 5],
            vec![1, 5],
        ]
        .into_iter()
        .collect();
        assert_eq!(expected_all_after_update, actual_all_after_update);

        let actual_all_from_a_after_update: HashSet<AnonymousGroundAtom> = runtime
            .query(&all_from_a)
            .unwrap()
            .into_iter()
            .collect();
        let expected_all_from_a_after_update: HashSet<AnonymousGroundAtom> = vec![
            vec![1, 2],
            vec![1, 3],
            vec![1, 4],
            vec![1, 5],
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

        let mut runtime = DyreRuntime::new(tc_program);
        vec![
            vec![1, 2],
            // this extra atom will help with testing that rederivation works
            vec![1, 5],
            vec![2, 3],
            vec![3, 4],
            vec![4, 5],
        ]
        .into_iter()
        .for_each(|edge| {
            runtime.push_fact("e", edge);
        });

        runtime.poll();

        let actual_all: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().collect();
        let expected_all: HashSet<AnonymousGroundAtom> = vec![
            vec![1, 2],
            vec![1, 5],
            vec![2, 3],
            vec![3, 4],
            // Second iter
            vec![1, 3],
            vec![2, 4],
            // Third iter
            vec![1, 4],
            // Fourth iter
            vec![4, 5],
            vec![3, 5],
            vec![2, 5],
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
            vec![1, 2],
            vec![1, 3],
            vec![1, 4],
            vec![1, 5],
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
            vec![1, 2],
            vec![2, 3],
            vec![3, 4],
            // Second iter
            vec![1, 3],
            vec![2, 4],
            // Third iter
            vec![1, 4],
            // This remains
            vec![1, 5],
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
            vec![1, 2],
            vec![1, 3],
            vec![1, 4],
            vec![1, 5],
        ]
        .into_iter()
        .collect();
        assert_eq!(
            expected_all_from_a_after_update,
            actual_all_from_a_after_update
        );
    }
}
*/