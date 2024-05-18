use std::collections::HashSet;
use dbsp::{CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, Stream};
use dbsp::operator::FilterMap;
use crate::builders::rule::RuleIdentifier;
use crate::engine::storage::{InternedConstantTerms, RelationIdentifier, StorageLayer};
use crate::interning::herbrand_universe::{InternedAtom, InternedRule};
use crate::rewriting::atom::{decode_fact, encode_atom_terms, encode_fact, EncodedAtom, is_encoded_atom_ground, project_encoded_atom, project_encoded_fact};
use crate::rewriting::rewrite::{apply_rewrite, EncodedRewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

fn compute_unique_column_sets(atoms: &Vec<InternedAtom>) -> Vec<(RelationIdentifier, Vec<usize>)> {
    let mut out = vec![];
    let mut variables: HashSet<usize> = Default::default();
    let mut fresh_variables: HashSet<usize> = Default::default();
    for body_atom in atoms {
        let index: Vec<_> = body_atom.1
            .iter()
            // A term that is 0, is not present
            .filter(|(is_var, term)| *term != 0)
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
        out.push((body_atom.0, index));

        fresh_variables.clear();
    }

    out
}

pub type ProjectedEncodedFact = EncodedAtom;
pub type BodyAtomPosition = usize;
pub type Weight = isize;
pub type FactSink = CollectionHandle<RelationIdentifier, (EncodedAtom, Weight)>;
pub type RuleSink = CollectionHandle<InternedRule, Weight>;
pub type FactSource = OutputHandle<OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>;
pub(crate) fn build_circuit() -> (DBSPHandle, FactSink, RuleSink, FactSource) {
    let (dbsp_runtime, ((fact_source, fact_sink), rule_sink)) =
    // Set the core count to whatever is available
        Runtime::init_circuit(8, |circuit| {
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
            let rules_by_id =
                rule_source.index_with(|(id, head, body)| (*id, ((head.0, encode_atom_terms(&head.1)), body.clone())));
            let iteration = rules_by_id.flat_map_index(|(rule_id, (_head, body))| {
                body.iter()
                    .enumerate()
                    .map(|(atom_position, atom)| ((*rule_id, atom_position), (atom.0, encode_atom_terms(&atom.1))))
                    .collect::<Vec<_>>()
            });
            let end_for_grounding = rule_source.index_with(|(id, head, body)| ((*id, body.len()), (head.0, encode_atom_terms(&head.1))));
            let empty_rewrites = rule_source.index_with(|(rule_id, _head, _body)| ((*rule_id, 0), EncodedRewrite::default()));

            let fact_index = fact_source
                .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                    Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                });

            let (indexed_inferences, _) = circuit
                .recursive(
                    |child,
                     (idb_index, rewrites): (
                         Stream<_, OrdIndexedZSet<(RelationIdentifier, ProjectedEncodedFact), EncodedAtom, Weight>>,
                         Stream<_, OrdIndexedZSet<(RuleIdentifier, BodyAtomPosition), EncodedRewrite, Weight>>,
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
    fact_sink: CollectionHandle<ProjectedEncodedFact, (EncodedAtom, Weight)>,
    rule_sink: CollectionHandle<InternedRule, Weight>,
    fact_source: OutputHandle<OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>,
}

impl ComputeLayer {
    pub fn new() -> Self {
        let (dbsp_runtime, fact_sink, rule_sink, fact_source) = build_circuit();

        Self { dbsp_runtime, fact_sink, rule_sink, fact_source }
    }
    pub fn send_fact(&self, hashed_relation_symbol: RelationIdentifier, interned_fact: &InternedConstantTerms) {
        self.fact_sink.push(hashed_relation_symbol, (encode_fact(&interned_fact), 1))
    }
    pub fn retract_fact(&self, hashed_relation_symbol: RelationIdentifier, interned_fact: &InternedConstantTerms) {
        self.fact_sink.push(hashed_relation_symbol, (encode_fact(&interned_fact), -1))
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
            .for_each(|(hashed_relation_symbol, fresh_fact, weight)| {
                let decoded_fact = decode_fact(fresh_fact);

                if weight.signum() > 0 {
                    storage_layer.push(&hashed_relation_symbol, decoded_fact);
                } else {
                    storage_layer.remove(&hashed_relation_symbol, &decoded_fact);
                }
            });
    }
}