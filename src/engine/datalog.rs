use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::time::Instant;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::builders::fact::Fact;
use crate::builders::goal::{Goal, pattern_match};
use crate::builders::rule::Rule;
use crate::engine::compute::{ComputeLayer, Weight};
use crate::interning::hash::reproducible_hash_one;
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
/// `MaterializedDatalogView` is both a **property graph** computation engine, incrementally maintaining recursive
/// and non-recursive queries, and a storage engine. The storage engine controls access to the materialisation by restricting it
/// to happen over two views:
/// 1. Consolidated - The always-up-to-date state of the materialisation.
/// 2. Frontier - The most recent updates. You can query the frontier each time after `poll` event happens
/// to retrieve the latest updates to the materialisation, and then store it in whichever database you are using.
///
/// # Examples
///
/// ```
/// use materialized_view::*;
///
/// type NodeIndex = i32;
/// type Edge = (NodeIndex, NodeIndex);
///
/// // The following recursive query is "equivalent" to this pseudo-SQL statement:
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
/// dynamic_view.push_fact("edge", (1, 2));
/// dynamic_view.push_fact("edge", (2, 3));
/// dynamic_view.push_fact("edge", (3, 4));
/// dynamic_view.push_fact("edge", (4, 5));
/// dynamic_view.push_fact("edge", (5, 6));
///
/// // Then poll to incrementally update the view
/// dynamic_view.poll();
///
/// // Confirm that 6 is reachable from 1
/// assert!(dynamic_view.contains("reaches", (1, 6)).unwrap());
///
/// // Retract a fact
/// dynamic_view.retract_fact("edge", (5, 6));
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
///     .push_rule((("reachableFromOne", (Const(1), Var("x"))), vec![("reaches", (Const(1), Var("x")))]));
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
///     .retract_rule((("reachableFromOne", (Const(1), Var("x"))), vec![("reaches", (Const(1), Var("x")))]));
/// dynamic_view.poll();
/// assert!(!dynamic_view.contains("reachableFromOne", (1, 5)).unwrap());
/// ```
pub struct MaterializedDatalogView {
    compute_layer: ComputeLayer,
    internment_layer: InternmentLayer,
    storage_layer: StorageLayer,
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
    /// Pushes a fact into the materialized view.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// reaches(?x, ?y) <- [edge(?x, ?y)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.poll();
    ///
    /// assert!(dynamic_view.contains("edge", (1, 2)).unwrap());
    /// assert!(dynamic_view.contains("reaches", (1, 2)).unwrap());
    ///
    /// ```
    pub fn push_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
        let interned_fact = self.internment_layer.intern_fact(fact.into());

        let encoded_fact = encode_fact(&interned_fact);
        self.compute_layer.send_fact(hashed_relation_symbol, encoded_fact);
        self.safe = false;

        self.storage_layer.contains(&hashed_relation_symbol, &encoded_fact)
    }
    /// Retracts facts from the materialized view.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// reaches(?x, ?y) <- [edge(?x, ?y)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.poll();
    /// assert!(dynamic_view.contains("edge", (1, 2)).unwrap());
    /// assert!(dynamic_view.contains("reaches", (1, 2)).unwrap());
    ///
    /// dynamic_view.retract_fact("edge", (1, 2));
    /// dynamic_view.poll();
    /// assert!(!dynamic_view.contains("edge", (1, 2)).unwrap());
    /// assert!(!dynamic_view.contains("reaches", (1, 2)).unwrap());
    ///
    /// let expected_update = vec![(-1, (1, 2))];
    /// let actual_update_edge: Vec<((isize, (i32, i32)))> = dynamic_view
    ///     .query_frontier_binary("edge", (ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|((x, y), weight)| (weight, (*x, *y)))
    ///     .collect();
    /// assert_eq!(expected_update, actual_update_edge);
    ///
    /// let actual_update_reaches: Vec<((isize, (i32, i32)))> = dynamic_view
    ///     .query_frontier_binary("reaches", (ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|((x, y), weight)| (weight, (*x, *y)))
    ///     .collect();
    /// assert_eq!(expected_update, actual_update_reaches);
    /// ```
    pub fn retract_fact(&mut self, relation_symbol: RelationSymbol, fact: impl Into<Fact>) -> bool {
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
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
    /// Pushes a rule into the materialized view.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// reaches(?x, ?y) <- [edge(?x, ?y)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.push_fact("edge", (2, 3));
    /// dynamic_view.poll();
    /// assert!(!dynamic_view.contains("reaches", (1, 3)).unwrap());
    ///
    /// dynamic_view.push_rule(rule!{ reaches(?x, ?z) <- [edge(?x, ?y), reaches(?y, ?z)] });
    /// dynamic_view.poll();
    /// assert!(dynamic_view.contains("reaches", (1, 3)).unwrap());
    ///
    /// ```
    pub fn push_rule(&mut self, rule: impl Into<Rule>) {
        let rule = rule.into();
        self.ensure_rule_relations_exist(&rule);
        self.compute_layer.send_rule(self.internment_layer.intern_rule(rule));

        self.safe = false;
    }
    /// Retracts a rule from the materialized view.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// reaches(?x, ?y) <- [edge(?x, ?y)],
    /// reaches(?x, ?z) <- [edge(?x, ?y), reaches(?y, ?z)]
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.push_fact("edge", (2, 3));
    /// dynamic_view.poll();
    /// assert!(dynamic_view.contains("reaches", (1, 3)).unwrap());
    ///
    /// dynamic_view.retract_rule(rule!{ reaches(?x, ?z) <- [edge(?x, ?y), reaches(?y, ?z)] });
    /// dynamic_view.poll();
    /// assert!(!dynamic_view.contains("reaches", (1, 3)).unwrap());
    /// ```
    pub fn retract_rule(&mut self, rule: impl Into<Rule>) {
        let rule = rule.into();
        if let Some(resolved_rule) = &self.internment_layer.resolve_rule(rule) {
            self.compute_layer.retract_rule(resolved_rule);
        }

        self.safe = false;
    }
    /// Checks whether a fact is true in the consolidated view. Throws an error if a poll is
    /// needed to obtain correct results.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// reaches(?x, ?y) <- [edge(?x, ?y)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.push_fact("edge", (2, 3));
    /// println!("{}", dynamic_view.contains("reaches", (2, 3)).unwrap_err());
    /// dynamic_view.poll();
    /// println!("{}", dynamic_view.contains("reaches", (2, 3)).unwrap());
    /// ```
    pub fn contains(
        &self,
        relation_symbol: RelationSymbol,
        fact: impl Into<Fact>,
    ) -> Result<bool, PollingError> {
        if !self.safe() {
            return Err(PollingError)
        }

        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
        if let Some(interned_fact) = self.internment_layer.resolve_fact(fact.into()) {
            return Ok(self.storage_layer.contains(&hashed_relation_symbol, &encode_fact(&interned_fact)))
        }

        Ok(false)
    }
    /// Queries the consolidated view of a unary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// rdfType(?o) <- [RDF(?s, "rdf:type", ?o)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("RDF", ("rust", "rdf:type", "programmingLanguage".to_string()));
    /// dynamic_view.poll();
    /// let query_result: Vec<((String,))> = dynamic_view
    ///     .query_unary::<String>("rdfType", (ANY_VALUE,))
    ///     .unwrap()
    ///     .map(|((x,))| (x.clone(),))
    ///     .collect();
    /// let expected_query_result = vec![("programmingLanguage".to_string(),)];
    /// assert_eq!(expected_query_result, query_result)
    /// ```
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
        let fact_storage = &self.storage_layer.get_relations(&hashed_relation_symbol).1;

        Ok(fact_storage
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms))
            .map(|encoded_fact| {
                let resolved_interned_constant_term = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();

                (resolved_interned_constant_term,)
            }))
    }
    /// Queries the frontier view of a unary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// rdfType(?o) <- [RDF(?s, "rdf:type", ?o)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("RDF", ("rust", "rdf:type", "programmingLanguage".to_string()));
    /// dynamic_view.poll();
    /// dynamic_view.push_fact("RDF", ("emacs", "rdf:type", "operatingSystem".to_string()));
    /// dynamic_view.poll();
    /// let query_result: Vec<(isize, (String,))> = dynamic_view
    ///     .query_frontier_unary::<String>("rdfType", (ANY_VALUE,))
    ///     .unwrap()
    ///     .map(|((x,), weight)| (weight, (x.clone(),)))
    ///     .collect();
    /// let expected_query_result = vec![(1, ("operatingSystem".to_string(),))];
    /// assert_eq!(expected_query_result, query_result)
    /// ```
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
        let frontier = &self.storage_layer.get_relations(&hashed_relation_symbol).0;

        Ok(frontier
            .iter()
            .filter(move |interned_constant_terms| pattern_match(&resolved_goal, &interned_constant_terms.0))
            .map(|(encoded_fact, weight)| {
                let resolved_interned_constant_term = self.internment_layer.resolve_interned_constant::<T>(decode_fact(*encoded_fact)[0]).unwrap();

                ((resolved_interned_constant_term,), *weight)
            }))
    }
    /// Queries the consolidated view of a binary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// subClassOf(?s, ?o) <- [RDF(?s, "rdfs:subClassOf", ?o)],
    /// subClassOf(?s, ?t) <- [subClassOf(?s, ?o), subClassOf(?o, ?t)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("RDF", ("animal".to_string(), "rdfs:subClassOf", "livingForm".to_string()));
    /// dynamic_view.push_fact("RDF", ("bird".to_string(), "rdfs:subClassOf", "animal".to_string()));
    /// dynamic_view.poll();
    /// let query_result: HashSet<((String, String))> = dynamic_view
    ///     .query_binary::<String, String>("subClassOf", (ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|((x, y))| (x.clone(), y.clone()))
    ///     .collect();
    /// let expected_query_result: HashSet<_> = vec![
    ///     ("animal".to_string(), "livingForm".to_string()),
    ///     ("bird".to_string(), "animal".to_string()),
    ///     ("bird".to_string(), "livingForm".to_string())].into_iter().collect();
    /// assert_eq!(expected_query_result, query_result);
    /// ```
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
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
    /// Queries the frontier of a binary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// subClassOf(?s, ?o) <- [RDF(?s, "rdfs:subClassOf", ?o)],
    /// subClassOf(?s, ?t) <- [subClassOf(?s, ?o), subClassOf(?o, ?t)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("RDF", ("animal".to_string(), "rdfs:subClassOf", "livingForm".to_string()));
    /// dynamic_view.push_fact("RDF", ("bird".to_string(), "rdfs:subClassOf", "animal".to_string()));
    /// dynamic_view.poll();
    /// dynamic_view.retract_fact("RDF", ("animal".to_string(), "rdfs:subClassOf", "livingForm".to_string()));
    /// dynamic_view.poll();
    /// let query_result: HashSet<((isize, (String, String)))> = dynamic_view
    ///     .query_frontier_binary::<String, String>("subClassOf", (ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|((x, y), weight)| (weight, (x.clone(), y.clone())))
    ///     .collect();
    /// let expected_query_result: HashSet<_> = vec![
    ///     (-1, ("animal".to_string(), "livingForm".to_string())),
    ///     (-1, ("bird".to_string(), "livingForm".to_string()))].into_iter().collect();
    /// assert_eq!(expected_query_result, query_result);
    /// ```
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
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
    /// Queries the consolidated view of a ternary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// triangle(?a, ?b, ?c) <- [edge(?a, ?b), edge(?b, ?c), edge(?c, ?a)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.push_fact("edge", (2, 3));
    /// dynamic_view.push_fact("edge", (3, 1));
    /// dynamic_view.push_fact("edge", (4, 5));
    /// dynamic_view.push_fact("edge", (5, 6));
    /// dynamic_view.poll();
    /// let query_result: HashSet<((i32, i32, i32))> = dynamic_view
    ///     .query_ternary::<i32, i32, i32>("triangle", (ANY_VALUE, ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|((a, b, c))| (*a, *b, *c))
    ///     .collect();
    /// let expected_query_result: HashSet<_> = vec![(1, 2, 3), (2, 3, 1), (3, 1, 2)].into_iter().collect();
    /// assert_eq!(expected_query_result, query_result);
    /// ```
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
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
    /// Queries the frontier of a ternary relation.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// use materialized_view::*;
    ///
    /// let query = program! {
    /// triangle(?a, ?b, ?c) <- [edge(?a, ?b), edge(?b, ?c), edge(?c, ?a)],
    /// };
    /// let mut dynamic_view = MaterializedDatalogView::new(query);
    ///
    /// dynamic_view.push_fact("edge", (1, 2));
    /// dynamic_view.push_fact("edge", (2, 3));
    /// dynamic_view.push_fact("edge", (3, 1));
    /// dynamic_view.push_fact("edge", (4, 5));
    /// dynamic_view.push_fact("edge", (5, 6));
    /// dynamic_view.poll();
    /// let query_result: HashSet<((isize, (i32, i32, i32)))> = dynamic_view
    ///     .query_frontier_ternary::<i32, i32, i32>("triangle", (ANY_VALUE, ANY_VALUE, ANY_VALUE))
    ///     .unwrap()
    ///     .map(|(((a, b, c), weight))| (weight, (*a, *b, *c)))
    ///     .collect();
    /// let expected_query_result: HashSet<_> = vec![(1, (1, 2, 3)), (1, (2, 3, 1)), (1, (3, 1, 2))].into_iter().collect();
    /// assert_eq!(expected_query_result, query_result);
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
        let hashed_relation_symbol = reproducible_hash_one(&relation_symbol);
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
        let now = Instant::now();
        self.compute_layer.step();
        println!("Step: {} ms", now.elapsed().as_millis());
    }
    fn consolidate(&mut self) {
        self.compute_layer.consolidate_into_storage_layer(&mut self.storage_layer)
    }
    /// Triggers a incremental materialization update
    pub fn poll(&mut self) {
        self.storage_layer.move_frontier();
        self.step();
        self.consolidate();
        self.safe = true;
    }
    /// Creates a new dynamic materialised view
    pub fn new(program: Vec<impl Into<Rule>>) -> Self {
        let storage_layer: StorageLayer = Default::default();
        let herbrand_universe = InternmentLayer::default();
        let compute_layer = ComputeLayer::new();
        let mut materialized_datalog_view = Self { compute_layer, internment_layer: herbrand_universe, storage_layer, safe: true };

        program.into_iter().for_each(|rule| {
            materialized_datalog_view.push_rule(rule);
        });

        materialized_datalog_view
    }
    /// Returns whether all incoming updates have been taken into account
    pub fn safe(&self) -> bool {
        self.safe
    }
    /// Returns the number of consolidated updates
    pub fn len(&self) -> usize {
        self.storage_layer.len()
    }
}

impl<'a, R> Extend<(&'a str, R)> for MaterializedDatalogView
where R: Into<Fact> {
    fn extend<T: IntoIterator<Item=(&'a str, R)>>(&mut self, iter: T) {
        iter
            .into_iter()
            .for_each(|(relation_name, fact)| { self.push_fact(relation_name, fact); });
    }
}

impl<R> Extend<R> for MaterializedDatalogView
    where R: Into<Rule> {
    fn extend<T: IntoIterator<Item=R>>(&mut self, iter: T) {
        iter
            .into_iter()
            .for_each(|rule| { self.push_rule(rule); });
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

    fn assemble_tc_program() -> Vec<Rule> {
        program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)]
        }
    }

    #[test]
    fn integration_test_push_fact() {
        let tc_program = assemble_tc_program();

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
        let tc_program = assemble_tc_program();

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
        let tc_program = assemble_tc_program();

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

        let actual_all_from_1_rule_dyn = rule::Rule::from((("tc1", (Const(1usize), Var("x"))), vec![("tc", (Const(1usize), Var("x")))]));
        let actual_all_from_1_rule_compiled = rule::Rule::from(rule!{ tc1(1usize, ?x) <- [tc(1usize, ?x)] });
        assert_eq!(actual_all_from_1_rule_dyn, actual_all_from_1_rule_compiled);
        materialized_datalog_view.push_rule(actual_all_from_1_rule_dyn);
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
        let tc_program = assemble_tc_program();

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
            t(?a, ?b, ?c) <- [e(?a, ?b), e(?b, ?c), e(?c, ?a)],
        };

        let mut materialized_datalog_view = MaterializedDatalogView::new(tc_program);
        vec![
            (1, 2),
            (2, 3),
            (3, 1),
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