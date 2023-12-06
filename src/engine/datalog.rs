use crate::engine::rewrite::{reliably_intern_rule, unify, InternedTerm, Rewrite};
use crate::engine::storage::RelationStorage;
use crate::evaluation::query::pattern_match;
use ahash::AHasher;
use datalog_syntax::*;
use dbsp::operator::FilterMap;
use dbsp::{
    CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OutputHandle, Runtime, Stream,
};
use lasso::{Key, Rodeo, Spur};
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
pub type FlattenedInternedFact = (usize, Vec<TypedValue>);
pub type FlattenedInternedAtom = (usize, Vec<InternedTerm>);
pub type FlattenedInternedRule = (usize, FlattenedInternedAtom, Vec<FlattenedInternedAtom>);

pub type Weight = isize;
pub struct ChibiRuntime {
    interner: Rodeo,
    dbsp_runtime: DBSPHandle,
    fact_sink: CollectionHandle<FlattenedInternedFact, Weight>,
    rule_sink: CollectionHandle<FlattenedInternedRule, Weight>,
    fact_source: OutputHandle<OrdIndexedZSet<usize, Vec<TypedValue>, Weight>>,
    materialisation: RelationStorage,
    safe: bool,
}

type Row = usize;

fn compute_atom_hash<'a>(key: impl Iterator<Item=&'a InternedTerm>) -> Row {
    let mut hasher = AHasher::default();

    key.for_each(|interned_term| {
        match interned_term {
            InternedTerm::Variable(_) => None::<TypedValue>.hash(&mut hasher),
            InternedTerm::Constant(inner) => Some(inner).hash(&mut hasher)
        }
    });

    hasher.finish() as Row
}
fn compute_fact_hash(fact: &[TypedValue]) -> Row {
    let mut hasher = AHasher::default();

    fact.iter().for_each(|constant| {
        constant.hash(&mut hasher)
    });

    hasher.finish() as Row
}

fn compute_projection_hash(fact: &[TypedValue], column_set: &Vec<usize>) -> Row {
    let mut hasher = AHasher::default();

    fact.iter().enumerate().for_each(|(idx, constant)| {
        if column_set.contains(&idx) {
            Some(constant).hash(&mut hasher);
        } else {
            None::<TypedValue>.hash(&mut hasher);
        }
    });

    hasher.finish() as Row
}

pub fn compute_unique_column_sets(rule: &FlattenedInternedRule) -> Vec<(usize, Vec<usize>)> {
    let mut out = vec![];
    let mut variables: HashSet<usize> = Default::default();
    let mut fresh_variables: HashSet<usize> = Default::default();
    for body_atom in &rule.2 {
        let index: Vec<_> = body_atom
            .1
            .iter()
            .enumerate()
            .flat_map(|(idx, term)| match term {
                InternedTerm::Variable(inner) => {
                    if !variables.contains(inner) {
                        fresh_variables.insert(*inner);
                        None
                    } else {
                        Some(idx)
                    }
                }
                InternedTerm::Constant(_) => Some(idx),
            })
            .collect();
        variables.extend(fresh_variables.iter());
        out.push((body_atom.0, index));

        fresh_variables.clear();
    }

    println!("{:?}", out);

    return out;
}

struct SliceDisplay<'a, T: 'a>(&'a [T]);

impl<'a, T: fmt::Debug + 'a> fmt::Debug for SliceDisplay<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut first = true;
        for item in self.0 {
            if !first {
                write!(f, ", {:?}", item)?;
            } else {
                write!(f, "{:?}", item)?;
            }
            first = false;
        }
        Ok(())
    }
}

fn is_ground(atom: &Vec<InternedTerm>) -> bool {
    return !atom.iter().any(|term| match term {
        InternedTerm::Variable(_) => true,
        InternedTerm::Constant(_) => false,
    });
}

impl ChibiRuntime {
    pub fn insert(&mut self, relation: &str, ground_atom: AnonymousGroundAtom) -> bool {
        let interned_symbol = self.interner.get_or_intern(relation);

        self.fact_sink
            .push((interned_symbol.into_usize(), ground_atom.clone()), 1);

        self.safe = false;

        self.materialisation.insert(relation, ground_atom)
    }
    pub fn remove(&mut self, query: &Query) -> impl Iterator<Item = AnonymousGroundAtom> {
        let mut removals = vec![];
        let query_symbol = query.symbol;
        let interned_symbol = self.interner.get(query_symbol).unwrap();

        let removal_targets: Vec<_> = self
            .materialisation
            .get_relation(query_symbol)
            .iter()
            .filter(|row| pattern_match(query, row))
            .cloned()
            .collect();

        removal_targets.iter().for_each(|candidate| {
            self.fact_sink.push(
                (
                    interned_symbol.into_usize(),
                    candidate.clone(),
                ),
                -1,
            );
        });

        self.safe = false;

        let mut relation = self.materialisation.inner.get_mut(query_symbol).unwrap();
        removal_targets.iter().for_each(|candidate| {
            relation.remove(candidate);
        });

        removal_targets.into_iter().for_each(|candidate| {
            removals.push(candidate)
        });

        removals.into_iter()
    }
    pub fn contains(
        &self,
        relation: &str,
        ground_atom: &AnonymousGroundAtom,
    ) -> Result<bool, String> {
        if !self.safe() {
            return Err("poll needed to obtain correct results".to_string());
        }

        Ok(self.materialisation.contains(relation, ground_atom))
    }
    pub fn query<'a>(
        &'a self,
        query: &'a Query,
    ) -> Result<impl Iterator<Item = &AnonymousGroundAtom> + 'a, String> {
        if !self.safe() {
            return Err("poll needed to obtain correct results".to_string());
        }
        return Ok(self
            .materialisation
            .get_relation(query.symbol)
            .iter()
            .map(|fact|fact)
            .filter(|fact| pattern_match(query, fact)));
    }
    pub fn poll(&mut self) {
        self.dbsp_runtime.step().unwrap();

        let consolidated = self.fact_source.consolidate();
        consolidated
            .iter()
            .for_each(|(relation_identifier, fresh_fact, weight)| {
                let spur = Spur::try_from_usize(relation_identifier).unwrap();
                let sym = self.interner.resolve(&spur);

                if weight.signum() > 0 {
                    self.materialisation.insert(sym, fresh_fact);
                } else {
                    self.materialisation.remove(sym, &fresh_fact);
                }
            });

        self.dbsp_runtime.dump_profile("./prof").unwrap();

        self.safe = true;
    }

    pub fn new(program: Program) -> Self {
        let materialisation: RelationStorage = Default::default();
        let mut relations = HashSet::new();

        let mut interner: Rodeo<Spur> = Rodeo::new();
        let (dbsp_runtime, ((fact_source, fact_sink), rule_sink)) =
            Runtime::init_circuit(8, |circuit| {
                let (rule_source, rule_sink) =
                    circuit.add_input_zset::<FlattenedInternedRule, Weight>();
                let (fact_source, fact_sink) =
                    circuit.add_input_zset::<FlattenedInternedFact, Weight>();

                let unique_column_sets = rule_source
                    .flat_map_index(compute_unique_column_sets)
                    .distinct();

                let hashed_facts_and_facts_by_symbol = fact_source
                    .map_index(|(fact_symbol, fact)| (*fact_symbol, (compute_fact_hash(fact), fact.clone())));

                let fact_index = hashed_facts_and_facts_by_symbol
                    .join_index(&unique_column_sets, |relation_symbol, (fact_hash, fact), column_set| {
                        Some(((*relation_symbol, compute_projection_hash(fact, column_set)), *fact_hash))
                    });

                let rules_by_id =
                    rule_source.index_with(|(id, head, body)| (*id, (head.clone(), body.clone())));

                let iteration = rules_by_id.flat_map_index(|(rule_id, (_head, body))| {
                    body.iter()
                        .enumerate()
                        .map(move |(atom_position, atom)| ((*rule_id, atom_position), atom.clone()))
                        .collect::<Vec<_>>()
                });

                let end_for_grounding = rule_source.index_with(|(id, head, body)| ((*id, body.len()), head.clone()));

                let start = rule_source.index_with(|(rule_id, _head, _body)| ((*rule_id, 0), Rewrite::default()));

                let facts_by_hashed_facts_and_symbol = hashed_facts_and_facts_by_symbol
                    .map_index(|(fact_symbol, (hashed_fact, fact))| ((*fact_symbol, *hashed_fact), fact.clone()));

                let (inferences, _, _) = circuit
                    .recursive(
                        |child,
                        (idb, idb_index, rewrites): (
                             Stream<_, OrdIndexedZSet<(usize, Row), Vec<TypedValue>, isize>>,
                             Stream<_, OrdIndexedZSet<(usize, Row), Row, isize>>,
                             Stream<_, OrdIndexedZSet<(usize, usize), Rewrite, isize>>,
                        )| {
                            let iteration = iteration.delta0(child);
                            let edb_index = fact_index.delta0(child);
                            let start = start.delta0(child);
                            let end_for_grounding = end_for_grounding.delta0(child);
                            let unique_column_sets = unique_column_sets.delta0(child);
                            let edb = facts_by_hashed_facts_and_symbol.delta0(child);

                            let current_rewrites = rewrites.join_index(
                                &iteration,
                                |key, rewrite, current_body_atom| {
                                    let fresh_atom = rewrite.apply(&current_body_atom.1);

                                    if !is_ground(&fresh_atom) {
                                        return Some((
                                            (current_body_atom.0, compute_atom_hash(fresh_atom.iter())),
                                            (*key, fresh_atom, rewrite.clone()),
                                        ));
                                    }

                                    return None;
                                },
                            );

                            let product_setup =
                                idb_index.join_index(&current_rewrites, |(relation_symbol, _projection), fact_hash, ((rule_id, atom_position), fresh_atom, rewrite)| {

                                    return Some(((*relation_symbol, *fact_hash), ((*rule_id, *atom_position + 1), fresh_atom.clone(), rewrite.clone())));
                                });

                            let cartesian_product = product_setup
                                .join_index(&idb, |(_relation_symbol, _fact_hash), ((rule_id, atom_position), fresh_atom, rewrite), fact| {
                                    let unification = unify(fresh_atom, fact).unwrap();
                                    let mut extended_sub = rewrite.clone();
                                    extended_sub.extend(unification);

                                    Some(((*rule_id, *atom_position), extended_sub))
                                });

                            let fresh_facts = end_for_grounding
                                .join_index(&cartesian_product, |(_rule_id, _final_atom_position), (head_atom_symbol, head_atom), final_substitution| {
                                    let fresh_fact = final_substitution.ground(head_atom);

                                    Some((*head_atom_symbol, fresh_fact))
                                });

                            let fresh_projections = fresh_facts
                                .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                                    Some(((*relation_symbol, compute_projection_hash(fact, column_set)), compute_fact_hash(fact)))
                                });

                            Ok((edb.plus(&fresh_facts.map_index(|(symbol, fresh_fact)| ((*symbol, compute_fact_hash(fresh_fact)), fresh_fact.clone()))), edb_index.plus(&fresh_projections), start.plus(&cartesian_product)))
                        },
                    )
                    .unwrap();

                Ok((((inferences.map_index(|((symbol, _), fresh_fact)| ((*symbol, fresh_fact.clone()))).output()), fact_sink), rule_sink))
            })
            .unwrap();

        program.inner.iter().for_each(|rule| {
            relations.insert(&rule.head.symbol);

            rule.body.iter().for_each(|body_atom| {
                relations.insert(&body_atom.symbol);
            });

            let interned_rule = reliably_intern_rule(rule.clone(), &mut interner);
            let flattened_head = (interned_rule.head.symbol, interned_rule.head.terms);
            let flattened_body = interned_rule
                .body
                .into_iter()
                .map(|atom| (atom.symbol, atom.terms))
                .collect();

            rule_sink.push((rule.id, flattened_head, flattened_body), 1);
        });

        relations.iter().for_each(|relation_symbol| {
            materialisation
                .inner
                .entry(relation_symbol.to_string())
                .or_default();
        });

        Self {
            interner,
            dbsp_runtime,
            fact_source,
            fact_sink,
            rule_sink,
            materialisation,
            safe: true,
        }
    }
    pub fn safe(&self) -> bool {
        self.safe
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::datalog::ChibiRuntime;
    use datalog_rule_macro::program;
    use datalog_syntax::*;
    use std::collections::HashSet;

    #[test]
    fn integration_test_insertions_only() {
        let tc_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)],
        };

        let mut runtime = ChibiRuntime::new(tc_program);
        vec![
            vec![1, 2],
            vec![2, 3],
            vec![3, 4],
        ]
        .into_iter()
        .for_each(|edge| {
            runtime.insert("e", edge);
        });

        runtime.poll();

        // This query reads as: "Get all in tc with any values in any positions"
        let all = build_query!(tc(_, _));
        // And this one as: "Get all in tc with the first term being a"
        // There also is a QueryBuilder, if you do not want to use a macro.
        let all_from_a = build_query!(tc(1, _));

        let actual_all: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().cloned().collect();
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
            runtime.query(&all_from_a).unwrap().cloned().collect();
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
        runtime.insert("e", vec![4, 5]);
        assert!(!runtime.safe());
        runtime.poll();
        assert!(runtime.safe());

        let actual_all_after_update: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().cloned().collect();
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
            .cloned()
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

        let mut runtime = ChibiRuntime::new(tc_program);
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
            runtime.insert("e", edge);
        });

        runtime.poll();

        let actual_all: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().cloned().collect();
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
            .cloned()
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
        let d_to_e = build_query!(e(4, 5));
        let deletions: Vec<_> = runtime.remove(&d_to_e).collect();
        assert!(!runtime.safe());
        runtime.poll();
        assert!(runtime.safe());

        let actual_all_after_update: HashSet<AnonymousGroundAtom> =
            runtime.query(&all).unwrap().cloned().collect();
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
            .cloned()
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
