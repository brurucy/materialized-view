use std::collections::HashSet;
use dbsp::{CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, Stream};
use dbsp::operator::FilterMap;
use crate::engine::storage::{InternedFact, StorageLayer};
use crate::interning::rule::{InternedRule, InternedTerm};
use crate::rewriting::atom::{decode_fact, encode_atom, encode_fact, EncodedAtom, is_encoded_atom_ground, project_encoded_atom, project_encoded_fact};
use crate::rewriting::rewrite::{apply_rewrite, EncodedRewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

pub type ProjectedEncodedFact = EncodedAtom;
pub type FlattenedInternedAtom = (usize, Vec<InternedTerm>);
pub type FlattenedInternedRule = (usize, FlattenedInternedAtom, Vec<FlattenedInternedAtom>);
pub type Weight = isize;

pub(crate) fn compute_unique_column_sets(rule: &FlattenedInternedRule) -> Vec<(usize, Vec<usize>)> {
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

    return out;
}


pub type FactSink = CollectionHandle<usize, (EncodedAtom, Weight)>;
pub type RuleSink = CollectionHandle<FlattenedInternedRule, Weight>;
pub type FactSource = OutputHandle<OrdZSet<(usize, EncodedAtom), Weight>>;
pub(crate) fn build_circuit() -> (DBSPHandle, FactSink, RuleSink, FactSource) {
    let (dbsp_runtime, ((fact_source, fact_sink), rule_sink)) =
        Runtime::init_circuit(8, |circuit| {
            let (rule_source, rule_sink) =
                circuit.add_input_zset::<FlattenedInternedRule, Weight>();
            let (fact_source, fact_sink) =
                circuit.add_input_indexed_zset::<usize, EncodedAtom, Weight>();

            let unique_column_sets = rule_source
                .flat_map_index(compute_unique_column_sets)
                .distinct();
            let rules_by_id =
                rule_source.index_with(|(id, head, body)| (*id, ((head.0, encode_atom(&head.1)), body.clone())));
            let iteration = rules_by_id.flat_map_index(|(rule_id, (_head, body))| {
                body.iter()
                    .enumerate()
                    .map(|(atom_position, atom)| ((*rule_id, atom_position), (atom.0, encode_atom(&atom.1))))
                    .collect::<Vec<_>>()
            });
            let end_for_grounding = rule_source.index_with(|(id, head, body)| ((*id, body.len()), (head.0, encode_atom(&head.1))));
            let empty_rewrites = rule_source.index_with(|(rule_id, _head, _body)| ((*rule_id, 0), EncodedRewrite::default()));

            let fact_index = fact_source
                .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                    Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                });

            let (indexed_inferences, _) = circuit
                .recursive(
                    |child,
                     (idb_index, rewrites): (
                         Stream<_, OrdIndexedZSet<(usize, ProjectedEncodedFact), EncodedAtom, isize>>,
                         Stream<_, OrdIndexedZSet<(usize, usize), EncodedRewrite, isize>>,
                     )| {
                        let iteration = iteration.delta0(child);
                        let edb_index = fact_index.delta0(child);
                        let empty_rewrites = empty_rewrites.delta0(child);
                        let end_for_grounding = end_for_grounding.delta0(child);
                        let unique_column_sets = unique_column_sets.delta0(child);

                        let previous_propagated_rewrites = rewrites.join_index(
                            &iteration,
                            |key, rewrite, (current_body_atom_symbol, current_body_atom)| {
                                let encoded_atom = apply_rewrite(&rewrite, &current_body_atom);

                                if !is_encoded_atom_ground(&encoded_atom) {
                                    return Some((
                                        (*current_body_atom_symbol, project_encoded_atom(&encoded_atom)),
                                        (*key, encoded_atom, *rewrite),
                                    ));
                                }

                                None
                            },
                        );

                        let rewrite_product =
                            idb_index.join_index(&previous_propagated_rewrites, |(_relation_symbol, _projected_fresh_atom), encoded_fact, ((rule_id, atom_position), encoded_fresh_atom, rewrite)| {
                                let unification = unify_encoded_atom_with_encoded_rewrite(*encoded_fresh_atom, *encoded_fact).unwrap();
                                let extended_rewrite = merge_right_rewrite_into_left(*rewrite, unification);

                                Some(((*rule_id, *atom_position + 1), extended_rewrite))
                            });

                        let fresh_facts = end_for_grounding
                            .join_index(&rewrite_product, |(_rule_id, _final_atom_position), (head_atom_symbol, head_atom), final_substitution| {
                                let fresh_encoded_fact = apply_rewrite(&final_substitution, head_atom);

                                Some((*head_atom_symbol, fresh_encoded_fact))
                            });

                        let fresh_indexed_facts = fresh_facts
                            .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                                Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                            });

                        let idb_index_out = edb_index.plus(&fresh_indexed_facts);
                        let rewrites_out = empty_rewrites.plus(&rewrite_product);

                        Ok((idb_index_out, rewrites_out))
                    },
                )
                .unwrap();

            let inferences_out = indexed_inferences
                .map(|((symbol, _fact_projection), fresh_fact)| (*symbol, *fresh_fact))
                .output();

            Ok(((inferences_out, fact_sink), rule_sink))
        })
            .unwrap();

    (dbsp_runtime, fact_sink, rule_sink, fact_source)
}

pub struct ComputeLayer {
    dbsp_runtime: DBSPHandle,
    fact_sink: CollectionHandle<usize, (EncodedAtom, Weight)>,
    rule_sink: CollectionHandle<FlattenedInternedRule, Weight>,
    fact_source: OutputHandle<OrdZSet<(usize, EncodedAtom), Weight>>,
}

pub fn flatten_rule(rule_id: usize, interned_rule: InternedRule) -> FlattenedInternedRule {
    let rule_id = rule_id;
    let flattened_head = (interned_rule.head.symbol, interned_rule.head.terms);
    let flattened_body = interned_rule.body.into_iter().map(|atom| (atom.symbol, atom.terms)).collect();

    return (rule_id, flattened_head, flattened_body)
}

impl ComputeLayer {
    pub fn new() -> Self {
        let (dbsp_runtime, fact_sink, rule_sink, fact_source) = build_circuit();

        Self { dbsp_runtime, fact_sink, rule_sink, fact_source }
    }
    pub fn send_fact(&self, interned_symbol: usize, interned_fact: &InternedFact) {
        self.fact_sink.push(interned_symbol, (encode_fact(&interned_fact), 1))
    }
    pub fn retract_fact(&self, interned_symbol: usize, interned_fact: &InternedFact) {
        self.fact_sink.push(interned_symbol, (encode_fact(&interned_fact), -1))
    }
    pub fn send_rule(&self, rule_id: u64, interned_rule: InternedRule) {
        self.rule_sink.push(flatten_rule(rule_id, interned_rule), 1)
    }
    pub fn retract_rule(&self, rule_id: u64, interned_rule: InternedRule) {
        self.rule_sink.push(flatten_rule(rule_id, interned_rule), -1)
    }
    pub fn step(&mut self) {
        self.dbsp_runtime.step().unwrap();
    }
    pub fn consolidate_into_storage_layer(&self, storage_layer: &mut StorageLayer) {
        self
            .fact_source
            .consolidate()
            .iter()
            .map(|((relation_identifier, encoded_fact), _, weight)| (relation_identifier, encoded_fact, weight))
            .for_each(|(relation_identifier, fresh_fact, weight)| {
                let sym = storage_layer.inner.get_index(relation_identifier).unwrap().0.clone();
                let decoded_fact = decode_fact(fresh_fact);

                if weight.signum() > 0 {
                    storage_layer.push(&sym, decoded_fact);
                } else {
                    storage_layer.remove(&sym, &decoded_fact);
                }
            });
    }
}