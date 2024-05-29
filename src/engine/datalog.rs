use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use ahash::RandomState;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::builders::fact::Fact;
use crate::builders::goal::{Goal, pattern_match};
use crate::builders::rule::Rule;
use crate::engine::compute::{ComputeLayer, Weight};
use crate::interning::hash::new_random_state;
use crate::interning::herbrand_universe::InternmentLayer;
use crate::rewriting::atom::{decode_fact, encode_fact};
/// Database-agnostic just-in-time iterative dynamic incremental materialized views for the masses!
/// * **Materialized View**: A query that gets updated once new data arrives
/// * **Incremental Materialized View**: Materialized views whose update latency is proportional to
/// the size of the update
/// * **Dynamic Incremental Materialized View**: Materialized views that can be "shortened" or "extended",
/// with the time taken to adjust to this change being proportional to the new updates generated from this change
/// * **Iterative dynamic incremental materialized views**: Dynamic materialized views that support recursive
/// statements (that of course also get incrementally maintained)
/// * **Database-agnostic just-in-time iterative dynamic incremental materialized views**: All that was said so far, with
/// zero compilation required **and** being easily integrated with any database engine!
///
/// `MaterializedDatalogView` is both a **graph** computation engine, incrementally maintaining recursive
/// and non-recursive queries, and a storage engine. The storage engine controls access to the materialisation by restricting it
/// to happen over two views:
/// 1. Consolidated - The always up to date state of the materialisation.
/// 2. Frontier - The most recent updates. You can query the frontier each time after a `poll` happens
/// to persist the materialisation in whichever database you are using.
///
/// # Examples
///
/// ```
/// use materialized_view::*;
///
/// type NodeIndex = isize;
/// type Edge = (NodeIndex, NodeIndex);
///
/// // The following recursive query is "equivalent" to this SQL statement:
/// // WITH RECURSIVE reaches(x, y) AS (
/// //    SELECT x, y FROM edge
/// //
/// //    UNION ALL
/// //
/// //    SELECT e.x, r.y
/// //    FROM edge e
/// //    JOIN reaches r ON e.y = r.x
/// // )
/// let recursive_query = program! {
/// reaches(?x, ?y) <- [edge(?x, ?y)],
/// reaches(?x, ?z) <- [edge(?x, ?y), reaches(?y, ?z)]
/// };
/// let mut dynamic_view = MaterializedDatalogView::new(recursive_query);
///
/// // Add some edges.
/// dynamic_view.push_fact("edge", (1isize, 2));
/// dynamic_view.push_fact("edge", (2isize, 3));
/// dynamic_view.push_fact("edge", (3isize, 4));
/// dynamic_view.push_fact("edge", (4isize, 5));
/// dynamic_view.push_fact("edge", (5isize, 6));
///
/// // Then poll to incrementally update the view
/// dynamic_view.poll();
///
/// // Confirm that 6 is reachable from 1
/// assert!(dynamic_view.contains("reaches", (1isize, 6)).unwrap());
///
/// // Retract a fact
/// dynamic_view.retract_fact("edge", (5isize, 6));
/// dynamic_view.poll();
///
/// // Query everything that is reachable from 1
/// dynamic_view
///     // The arity of the relation being queried must be specified. e.g to query
///     // a relation with two columns, `query_binary` ought to be used.
///     .query_binary::<NodeIndex, NodeIndex>("reaches", (Some(1), ANY_VALUE))
///     .unwrap()
///     .for_each(|edge| println!("{} is reachable from 1", *edge.1));
///
/// // You are also able to query only the __most recent__ updates.
/// dynamic_view
///     // The arity of the relation being queried must be specified. e.g to query
///     // a relation with two columns, `query_binary` ought to be used.
///     .query_frontier_binary::<NodeIndex, NodeIndex>("reaches", (Some(1), ANY_VALUE))
///     .unwrap()
///     // The second argument is the weight. It represents whether the given value should be added
///     // or retracted.
///     .for_each(|((from, to), weight)| println!("Diff: {} - Value: ({}, {})", weight, *from, *to));
///
/// // By extending the query with another query, it is possible to incrementally query the incrementally
/// // maintained queries
/// dynamic_view
///     // Queries can also be assembled both a macro a-la program! called rule!:
///     // rule! { reachableFromOne(1isize, ?x) <- reaches(1isize, ?x) }
///     .push_rule((("reachableFromOne", (Const(1isize), Var("x"))), vec![("reaches", (Const(1isize), Var("x")))]));
///
/// dynamic_view.poll();
/// dynamic_view
///     .query_binary::<NodeIndex, NodeIndex>("reachableFromOne", (Some(1), ANY_VALUE))
///     .unwrap()
///     .for_each(|edge| println!("{} is reachable from 1", edge.1));
///
/// // And of course, you can retract rules as well!
/// assert!(dynamic_view.contains("reachableFromOne", (1, 5)).unwrap());
/// dynamic_view
///     .retract_rule((("reachableFromOne", (Const(1isize), Var("x"))), vec![("reaches", (Const(1isize), Var("x")))]));
/// dynamic_view.poll();
/// assert!(!dynamic_view.contains("reachableFromOne", (1, 5)).unwrap());
/// ```
pub struct MaterializedDatalogView {
    compute_layer: ComputeLayer,
    internment_layer: InternmentLayer,
    storage_layer: StorageLayer,
    rs: RandomState,
    safe: bool,
}

pub const EMPTY_PROGRAM: Vec<Rule> = vec![];

type RelationSymbol<'a> = &'a str;

pub struct PollingError;

impl Display for PollingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("polling is needed to obtain correct results")
    }
}

impl Debug for PollingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("polling is needed to obtain correct results")
    }
}

impl Error for PollingError {}

impl MaterializedDatalogView {
    pub fn push_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let interned_fact = self.internment_layer.intern_fact(fact.into());

        let encoded_fact = encode_fact(&interned_fact);
        self.compute_layer.send_fact(hashed_relation_symbol, encoded_fact);
        self.safe = false;

        self.storage_layer.contains(&hashed_relation_symbol, &encoded_fact)
    }
    pub fn retract_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        if let Some(resolved_fact) = self.internment_layer.resolve_fact(fact.into()) {
            let encoded_resolved_fact = encode_fact(&resolved_fact);

            self.compute_layer.retract_fact(hashed_relation_symbol, encoded_resolved_fact);
            self.safe = false;

            return self.storage_layer.contains(&hashed_relation_symbol, &encoded_resolved_fact)
        };

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
        if let Some(resolved_rule) = &self.internment_layer.resolve_rule(rule) {
            self.compute_layer.retract_rule(resolved_rule);
        }

        self.safe = false;
    }
    pub fn contains(
        &self,
        relation_symbol: RelationSymbol,
        fact: impl Into<Fact>,
    ) -> Result<bool, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        if let Some(interned_fact) = self.internment_layer.resolve_fact(fact.into()) {
            return Ok(self.storage_layer.contains(&hashed_relation_symbol, &encode_fact(&interned_fact)))
        }

        Ok(false)
    }
    pub fn query_unary<'a, T: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=(&T, )>, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = &self.storage_layer.get_relations(&hashed_relation_symbol).1;

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|encoded_fact| {
                let resolved_interned_constant_term = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();

                (resolved_interned_constant_term,)
            }))
    }
    pub fn query_frontier_unary<'a, T: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=((&T, ), Weight)>, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let frontier = &self.storage_layer.get_relations(&hashed_relation_symbol).0;

        Ok(frontier
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms.0))
            .map(|(encoded_fact, weight)| {
                let resolved_interned_constant_term = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();

                ((resolved_interned_constant_term,), *weight)
            }))
    }
    pub fn query_binary<'a, T: 'static, R: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=(&T, &R)>, PollingError> {
        if !self.safe() {
            return Err(PollingError);
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = &self.storage_layer.get_relations(&hashed_relation_symbol).1;

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|encoded_fact| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_interned_constant::<R>(decode_fact(*encoded_fact)[1]).unwrap();

                (resolved_interned_constant_term_one, resolved_interned_constant_term_two)
            }))
    }
    pub fn query_frontier_binary<'a, T: 'static, R: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=((&T, &R), Weight)>, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let frontier = &self.storage_layer.get_relations(&hashed_relation_symbol).0;

        Ok(frontier
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms.0))
            .map(|(encoded_fact, weight)| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_interned_constant::<R>(decode_fact(*encoded_fact)[1]).unwrap();

                ((resolved_interned_constant_term_one, resolved_interned_constant_term_two), *weight)
            }))
    }
    pub fn query_ternary<'a, T: 'static, R: 'static, S: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=(&T, &R, &S)>, PollingError> {
        if !self.safe() {
            return Err(PollingError);
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let fact_storage = &self.storage_layer.get_relations(&hashed_relation_symbol).1;

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|encoded_fact| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_interned_constant::<R>(decode_fact(*encoded_fact)[1]).unwrap();
                let resolved_interned_constant_term_three = self.internment_layer.resolve_interned_constant::<S>(decode_fact(*encoded_fact)[2]).unwrap();

                (resolved_interned_constant_term_one, resolved_interned_constant_term_two, resolved_interned_constant_term_three)
            }))
    }
    pub fn query_frontier_ternary<'a, T: 'static, R: 'static, S: 'static>(
        &self,
        relation_symbol: RelationSymbol,
        goal: impl Into<Goal>,
    ) -> Result<impl Iterator<Item=((&T, &R, &S), Weight)>, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let goal = goal.into();
        let resolved_goal = self.internment_layer.resolve_goal(goal).unwrap();
        let hashed_relation_symbol = self.rs.hash_one(&relation_symbol);
        let frontier = &self.storage_layer.get_relations(&hashed_relation_symbol).0;

        Ok(frontier
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms.0))
            .map(|(encoded_fact, weight)| {
                let resolved_interned_constant_term_one = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();
                let resolved_interned_constant_term_two = self.internment_layer.resolve_interned_constant::<R>(decode_fact(*encoded_fact)[1]).unwrap();
                let resolved_interned_constant_term_three = self.internment_layer.resolve_interned_constant::<S>(decode_fact(*encoded_fact)[2]).unwrap();

                ((resolved_interned_constant_term_one, resolved_interned_constant_term_two, resolved_interned_constant_term_three), *weight)
            }))
    }
    fn step(&mut self) {
        self.compute_layer.step();
    }
    fn consolidate(&mut self) {
        self.compute_layer.consolidate_into_storage_layer(&mut self.storage_layer)
    }
    pub fn poll(&mut self) {
        self.storage_layer.move_frontier();
        self.step();
        self.consolidate();
        self.safe = true;
    }
    pub fn new(program: Vec<impl Into<Rule>>) -> Self {
        let storage_layer: StorageLayer = Default::default();
        let compute_layer = ComputeLayer::new();
        let herbrand_universe = InternmentLayer::default();
        let mut materialized_datalog_view = Self { compute_layer, internment_layer: herbrand_universe, storage_layer, rs: new_random_state(), safe: true };

        program.into_iter().for_each(|rule| {
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
    use crate::engine::datalog::{EMPTY_PROGRAM, MaterializedDatalogView};
    use datalog_syntax_macros::{program, rule};
    use datalog_syntax::*;
    use std::collections::HashSet;
    use crate::builders::goal::ANY_VALUE;
    use crate::builders::rule;
    use crate::builders::rule::{Const, Var};

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

        // Update
        materialized_datalog_view.push_fact("e", (4usize, 5usize));
        assert!(!materialized_datalog_view.safe());
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

        let mut materialized_datalog_view = MaterializedDatalogView::new(tc_program);
        vec![
            (1, 2),
            // this extra atom will help with testing that rederivation works
            (1, 5),
            (2, 3),
            (3, 4),
            (4, 5),
        ]
        .into_iter()
        .for_each(|edge: Edge| {
            materialized_datalog_view.push_fact("e", edge);
        });
        materialized_datalog_view.poll();

        let actual_all: HashSet<Edge> =
            materialized_datalog_view.query_binary("tc", (ANY_VALUE, ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
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

        let actual_all_from_a: HashSet<Edge> = materialized_datalog_view
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
        materialized_datalog_view.retract_fact("e", (4usize, 5usize));
        assert!(!materialized_datalog_view.safe());
        materialized_datalog_view.poll();
        assert!(materialized_datalog_view.safe());

        let actual_all_after_update: HashSet<Edge> =
            materialized_datalog_view.query_binary("tc", (ANY_VALUE, ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
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
    fn integration_test_push_rule() {
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

        let actual_all_from_a_rule_dyn = rule::Rule::from((("tc1", (Const(1usize), Var("x"))), vec![("tc", (Const(1usize), Var("x")))]));
        let actual_all_from_a_rule_compiled = rule::Rule::from(rule!{ tc1(1usize, ?x) <- [tc(1usize, ?x)] });
        assert_eq!(actual_all_from_a_rule_dyn, actual_all_from_a_rule_compiled);
        materialized_datalog_view.push_rule(actual_all_from_a_rule_dyn);
        assert!(!materialized_datalog_view.safe());
        materialized_datalog_view.poll();
        assert!(materialized_datalog_view.safe());

        let actual_all_from_1: HashSet<Edge> =
            materialized_datalog_view.query_binary("tc1", (ANY_VALUE, ANY_VALUE)).unwrap().map(|(x, y)| (*x, *y)).collect();
        let expected_all_from_1: HashSet<Edge> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
        ]
            .into_iter()
            .collect();
        assert_eq!(expected_all_from_1, actual_all_from_1);

        expected_all_from_1.iter().for_each(|fact| {
            assert!(materialized_datalog_view.contains("tc", *fact).unwrap());
            assert!(materialized_datalog_view.contains("tc1", *fact).unwrap());
        });

        materialized_datalog_view.push_fact("e", (4usize, 5usize));
        materialized_datalog_view.poll();

        let actual_all_from_1_after_update: HashSet<Edge> = materialized_datalog_view
            .query_binary("tc1", (ANY_VALUE, ANY_VALUE))
            .unwrap()
            .map(|(x, y)| (*x, *y))
            .collect();
        let expected_all_from_1_after_update: HashSet<Edge> = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ]
            .into_iter()
            .collect();
        assert_eq!(
            expected_all_from_1_after_update,
            actual_all_from_1_after_update
        );
    }
    #[test]
    fn integration_test_retract_rule() {
        let tc_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)],
        };

        let mut materialized_datalog_view = MaterializedDatalogView::new(EMPTY_PROGRAM);
        vec![
            (1, 2),
            (2, 3),
            (3, 4),
        ]
            .into_iter()
            .for_each(|edge: Edge| {
                materialized_datalog_view.push_fact("e", edge);
            });
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 3);
        materialized_datalog_view.push_rule(tc_program[0].clone());
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 6);
        materialized_datalog_view.push_rule(tc_program[1].clone());
        materialized_datalog_view.poll();

        assert_eq!(materialized_datalog_view.len(), 9);

        materialized_datalog_view.push_rule(rule!{ tc1(1usize, ?x) <- [tc(1usize, ?x)] });
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 12);

        vec![
            (1, 2),
            (1, 3),
            (1, 4),
        ]
            .into_iter()
            .for_each(|fact: Edge| {
            assert!(materialized_datalog_view.contains("tc", fact).unwrap());
            assert!(materialized_datalog_view.contains("tc1", fact).unwrap());
        });

        materialized_datalog_view.retract_rule(rule!{ tc1(1usize, ?x) <- [tc(1usize, ?x)] });
        assert!(!materialized_datalog_view.safe);
        materialized_datalog_view.poll();
        assert!(materialized_datalog_view.safe);
        assert_eq!(materialized_datalog_view.len(), 9);
        materialized_datalog_view.retract_rule(rule!{ tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)] });
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 6);
        materialized_datalog_view.retract_rule(rule!{ tc(?x, ?y) <- [e(?x, ?y)] });
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 3);
    }

    #[test]
    fn integration_test_triangle_query() {
        let tc_program = program! {
            //tc(?a, ?b) <- [e(?a, ?b), e(?b, ?c)],
            t(?a, ?b, ?c) <- [e(?a, ?b), e(?b, ?c), e(?c, ?a)],
        };

        let mut materialized_datalog_view = MaterializedDatalogView::new(tc_program);
        vec![
            (1, 2),
            (2, 3),
            (3, 1),
            //
            (4, 5),
            (5, 6)
        ]
            .into_iter()
            .for_each(|edge: Edge| {
                materialized_datalog_view.push_fact("e", edge);
            });
        materialized_datalog_view.poll();
        assert_eq!(materialized_datalog_view.len(), 8);

        type Triangle = (NodeIndex, NodeIndex, NodeIndex);
        let actual_all: HashSet<Triangle> =
            materialized_datalog_view
                .query_ternary("t", (ANY_VALUE, ANY_VALUE, ANY_VALUE))
                .unwrap()
                .map(|(x, y, z)| (*x, *y, *z))
                .collect();
        let expected_all: HashSet<Triangle> = vec![
            (1, 2, 3),
            (2, 3, 1),
            (3, 1, 2),
        ]
            .into_iter()
            .collect();
        assert_eq!(expected_all, actual_all);
    }
}