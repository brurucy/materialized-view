use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use dbsp::{CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, Stream};
use dbsp::operator::FilterMap;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::interning::hash::new_random_state;
use crate::interning::herbrand_universe::{InternedAtom, InternedRule};
use crate::rewriting::atom::{encode_atom_terms, EncodedAtom, project_encoded_atom, project_encoded_fact};
use crate::rewriting::rewrite::{apply_rewrite, EncodedRewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

fn compute_unique_column_sets(atoms: &Vec<InternedAtom>) -> Vec<(RelationIdentifier, Vec<usize>)> {
    let mut out = std::vec![];
    let mut variables: HashSet<usize> = Default::default();
    let mut fresh_variables: HashSet<usize> = Default::default();
    for body_atom in atoms {
        let index: Vec<_> = body_atom.1
            .iter()
            .filter(|(_is_var, term)| *term != 0)
            .enumerate()
            .flat_map(|(idx, (is_var, inner))| match is_var {
                true => {
                    if !variables.contains(inner) {
                        fresh_variables.insert(*inner);
                        None
                    } else {
                        Some(idx)
                    }
                }
                _ => Some(idx),
            })
            .collect();
        variables.extend(fresh_variables.iter());
        out.push((body_atom.0 as u64, index));

        fresh_variables.clear();
    }

    out
}

pub(crate) type ProjectedEncodedFact = EncodedAtom;
pub(crate) type Weight = isize;
pub(crate) type FactSink = CollectionHandle<RelationIdentifier, (EncodedAtom, Weight)>;
pub(crate) type RuleSink = CollectionHandle<InternedRule, Weight>;
pub(crate) type FactSource = OutputHandle<OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>;
pub(crate) type LastHash = u64;
pub(crate) type NewHash = u64;
pub(crate) fn build_circuit() -> (DBSPHandle, FactSink, RuleSink, FactSource) {
    let (dbsp_runtime, ((fact_source, fact_sink), rule_sink)) =
        Runtime::init_circuit(std::thread::available_parallelism().unwrap().get(), |circuit| {
            let (rule_source, rule_sink) =
                circuit.add_input_zset::<InternedRule, Weight>();
            let (fact_source, fact_sink) =
                circuit.add_input_indexed_zset::<RelationIdentifier, EncodedAtom, Weight>();

            let unique_column_sets = rule_source
                .map(|flattened_rule| {
                    let head = flattened_rule.1;
                    let mut body = flattened_rule.2.clone();
                    body.push(head);

                    body
                })
                .flat_map_index(compute_unique_column_sets)
                .distinct();

            let rules_by_id = rule_source.map(|(_id, head, body)| (head.clone(), body.clone()));

            let iteration = rules_by_id
                .flat_map_index(|(_head, body)| {
                let mut body_subsets = vec![];
                let mut last_hash =0;

                for i in 0..body.len() {
                    let mut rs = new_random_state().build_hasher();
                    last_hash.hash(&mut rs);
                    body[i].hash(&mut rs);
                    let this_hash = rs.finish();
                    body_subsets.push((last_hash as LastHash, (this_hash as NewHash, (body[i].0, encode_atom_terms(&body[i].1)))));
                    last_hash = this_hash;
                }

                body_subsets
            })
                .distinct();

            let end_for_grounding = rules_by_id.index_with(|(head, body)| {
                let mut last_hash =0;
                for i in 0..body.len() {
                    let mut rs = new_random_state().build_hasher();
                    last_hash.hash(&mut rs);
                    body[i].hash(&mut rs);
                    let this_hash = rs.finish();
                    last_hash = this_hash;
                }

                (last_hash as LastHash, (0_u64 as NewHash, (head.0, encode_atom_terms(&head.1))))
            });
            let empty_rewrite = EncodedRewrite::default();
            let empty_rewrites = rule_source
                .index_with(move |(_rule_id, _head, _body)| (0u64, empty_rewrite))
                .distinct();

            let fact_index = fact_source
                .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                    Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                });

            let (_, edb_union_idb, _) = circuit
                .recursive(
                    |child,
                     (idb_index, _edb_union_idb, rewrites): (
                         Stream<_, OrdIndexedZSet<(RelationIdentifier, ProjectedEncodedFact), EncodedAtom, Weight>>,
                         Stream<_, OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>,
                         Stream<_, OrdIndexedZSet<LastHash, EncodedRewrite, Weight>>,
                     )| {
                        let iteration = iteration.delta0(child);
                        let edb_index = fact_index.delta0(child);
                        let edb = fact_source.delta0(child);
                        let empty_rewrites = empty_rewrites.delta0(child);
                        let end_for_grounding = end_for_grounding.delta0(child);
                        let unique_column_sets = unique_column_sets.delta0(child);

                        let previous_propagated_rewrites = rewrites.join_index(
                            &iteration,
                            |_key, rewrite, (new_hash, (current_body_atom_symbol, current_body_atom))| {
                                let encoded_atom = apply_rewrite(rewrite, &current_body_atom);

                                Some(((*current_body_atom_symbol, project_encoded_atom(&encoded_atom)), (*new_hash, encoded_atom, *rewrite), ))
                            },
                        );

                        let rewrite_product =
                            idb_index.join_index(&previous_propagated_rewrites, |(_relation_symbol, _projected_fresh_atom), encoded_fact, (new_hash, encoded_fresh_atom, rewrite)| {
                                let unification = unify_encoded_atom_with_encoded_rewrite(*encoded_fresh_atom, *encoded_fact).unwrap();
                                let extended_rewrite = merge_right_rewrite_into_left(*rewrite, unification);

                                Some((*new_hash, extended_rewrite))
                            });

                        let fresh_facts = end_for_grounding
                            .join_index(&rewrite_product, |_last_hash, (_new_hash, (head_atom_symbol, head_atom)), final_substitution| {
                                let fresh_encoded_fact = apply_rewrite(final_substitution, head_atom);

                                Some((*head_atom_symbol, fresh_encoded_fact))
                            });

                        let fresh_indexed_facts = fresh_facts
                            .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                                Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                            });

                        let idb_index_out = edb_index.plus(&fresh_indexed_facts);
                        let edb_union_idb_out = edb.plus(&fresh_facts);
                        let rewrites_out = empty_rewrites.plus(&rewrite_product);

                        Ok((idb_index_out, edb_union_idb_out.map(|(key, value)| (*key, *value)), rewrites_out))
                    },
                )
                .unwrap();

            let inferences_out = edb_union_idb
                .distinct()
                .output();

            Ok(((inferences_out, fact_sink), rule_sink))
        })
            .unwrap();

    (dbsp_runtime, fact_sink, rule_sink, fact_source)
}

pub struct ComputeLayer {
    dbsp_runtime: DBSPHandle,
    fact_sink: CollectionHandle<ProjectedEncodedFact, (EncodedAtom, Weight)>,
    rule_sink: CollectionHandle<InternedRule, Weight>,
    fact_source: OutputHandle<OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>,
}

impl ComputeLayer {
    pub fn new() -> Self {
        let (dbsp_runtime, fact_sink, rule_sink, fact_source) = build_circuit();

        Self { dbsp_runtime, fact_sink, rule_sink, fact_source }
    }
    pub fn send_fact(&self, hashed_relation_symbol: RelationIdentifier, encoded_fact: EncodedAtom) {
        self.fact_sink.push(hashed_relation_symbol, (encoded_fact, 1))
    }
    pub fn retract_fact(&self, hashed_relation_symbol: RelationIdentifier, encoded_fact: EncodedAtom) {
        self.fact_sink.push(hashed_relation_symbol, (encoded_fact, -1))
    }
    pub fn send_rule(&self, rule: InternedRule) {
        self.rule_sink.push(rule, 1)
    }
    pub fn retract_rule(&self, rule: &InternedRule) {
        self.rule_sink.push(rule.clone(), -1)
    }
    pub fn step(&mut self) {
        self.dbsp_runtime.step().unwrap();
    }
    pub fn consolidate_into_storage_layer(&self, storage_layer: &mut StorageLayer) {
        self
            .fact_source
            .consolidate()
            .iter()
            .map(|((hashed_relation_symbol, encoded_fact), _, weight)| (hashed_relation_symbol, encoded_fact, weight))
            .for_each(|(hashed_relation_symbol, encoded_fact, weight)| {
                storage_layer.update(&hashed_relation_symbol, encoded_fact, weight);
            });
    }
}