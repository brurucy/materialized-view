use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use dbsp::{ChildCircuit, Circuit, CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OrdZSet, OutputHandle, RootCircuit, Runtime, Stream};
use dbsp::operator::FilterMap;
use crate::engine::storage::{RelationIdentifier, StorageLayer};
use crate::interning::hash::new_random_state;
use crate::interning::herbrand_universe::{InternedAtom, InternedRule};
use crate::rewriting::atom::{encode_atom_terms, EncodedAtom, project_encoded_atom, project_encoded_fact};
use crate::rewriting::rewrite::{apply_rewrite, EncodedRewrite, merge_right_rewrite_into_left, unify_encoded_atom_with_encoded_rewrite};

fn compute_unique_column_sets(atoms: &Vec<InternedAtom>) -> Vec<(RelationIdentifier, Vec<usize>)> {
    let mut out = vec![];
    let mut variables: HashSet<usize> = Default::default();
    let mut fresh_variables: HashSet<usize> = Default::default();
    for body_atom in atoms {
        let index: Vec<_> = body_atom.1
            .iter()
            // A term that is 0, is not present
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
pub type LastHash = u64;
pub type NewHash = u64;
pub(crate) fn build_circuit() -> (DBSPHandle, FactSink, RuleSink, FactSource) {
    let (dbsp_runtime, ((fact_source, fact_sink), rule_sink)) =
    // TODO! Set the core count to whatever is available
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

            let rules_by_id = rule_source.map(|(id, head, body)| (head.clone(), body.clone()));

            let iteration = rules_by_id
                .flat_map_index(|(head, body)| {
                let mut body_subsets = vec![];
                let mut last_hash =0;

                for i in 0..body.len() {
                    let mut rs = new_random_state().build_hasher();
                    last_hash.hash(&mut rs);
                    body[i].hash(&mut rs);
                    let this_hash = rs.finish(); // please use interner instead for collision resistance
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
                    let this_hash = rs.finish(); // please use interner instead for collision resistance
                    last_hash = this_hash;
                }

                (last_hash as LastHash, (0_u64 as NewHash, (head.0, encode_atom_terms(&head.1))))
            });
            let empty_rewrites = rule_source
                .index_with(|(rule_id, _head, _body)| (0u64, EncodedRewrite::default()))
                .distinct();
            // 0 - tc(?x, ?y) <- e(?x, ?y) -- e(?x, ?y)
            // 1 - tc(?x, ?z) <- e(?x, ?y) -- e(?x, ?y), tc(?y, ?z) --
            // ((0, 0), {})
            // ((1, 0), {})

            // tc(a, b) // 1 <- e(a, b) // 0
            // tc(a, b) // 2 <- e(a, b) // 0, e(b, c) // 1
            // t(a, b, c) // 2 <- e(a, b) // 0, e(b, c) // 1, e(c, a) // 2

            let fact_index = fact_source
                .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                    Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                });

            let traces = circuit.fixedpoint(|child| {
                let (vars, input_streams) = dbsp::operator::recursive::RecursiveStreams::<_>::new(child);
                let output_streams = (|child,
                                       (idb_index, _edb_union_idb, rewrites): (
                                           Stream<_, OrdIndexedZSet<(RelationIdentifier, ProjectedEncodedFact), EncodedAtom, Weight>>,
                                           Stream<_, OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>,
                                           Stream<_, OrdIndexedZSet<LastHash, EncodedRewrite, Weight>>, // TODO: type alias the u64 to bodyprefixhash or w/e name
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
                            let encoded_atom = apply_rewrite(&rewrite, &current_body_atom);

                            Some(((*current_body_atom_symbol, project_encoded_atom(&encoded_atom)), (*new_hash, encoded_atom, *rewrite), ))
                        },
                    );

                    let idb_index = edb_index
                        .plus(&idb_index)
                        .distinct();
                    let rewrite_product =
                        idb_index.join_index(&previous_propagated_rewrites, |(_relation_symbol, _projected_fresh_atom), encoded_fact, (new_hash, encoded_fresh_atom, rewrite)| {
                            let unification = unify_encoded_atom_with_encoded_rewrite(*encoded_fresh_atom, *encoded_fact).unwrap();
                            let extended_rewrite = merge_right_rewrite_into_left(*rewrite, unification);

                            Some((*new_hash, extended_rewrite))
                        }).distinct();

                    let fresh_facts = end_for_grounding
                        .join_index(&rewrite_product, |_last_hash, (_new_hash, (head_atom_symbol, head_atom)), final_substitution| {
                            let fresh_encoded_fact = apply_rewrite(&final_substitution, head_atom);

                            Some((*head_atom_symbol, fresh_encoded_fact))
                        });

                    let fresh_indexed_facts = fresh_facts
                        .join_index(&unique_column_sets, |relation_symbol, fact, column_set| {
                            Some(((*relation_symbol, project_encoded_fact(fact, column_set)), *fact))
                        });
                        /*.distinct()*/;

                    let idb_index_out = fresh_indexed_facts;
                    let edb_union_idb_out = edb.plus(&fresh_facts)
                        .distinct();
                    let rewrites_out = empty_rewrites.plus(&rewrite_product);

                    let stream = edb_union_idb_out.map(|(key, value)| (*key, *value));

                    Ok((idb_index_out, stream, rewrites_out))
                })(child, input_streams)?;
                //let output_streams = dbsp::operator::recursive::RecursiveStreams::<_>::distinct(output_streams);
                dbsp::operator::recursive::RecursiveStreams::<_>::connect(&output_streams, vars);
                Ok(dbsp::operator::recursive::RecursiveStreams::<_>::export(output_streams))
            }).unwrap();
            let (_, edb_union_idb, _): (_, Stream<_, OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>, _) = <(
                Stream<_, OrdIndexedZSet<(RelationIdentifier, ProjectedEncodedFact), EncodedAtom, Weight>>,
                Stream<_, OrdZSet<(RelationIdentifier, EncodedAtom), Weight>>,
                Stream<_, OrdIndexedZSet<LastHash, EncodedRewrite, Weight>>, // TODO: type alias the u64 to bodyprefixhash or w/e name
            ) as dbsp::operator::recursive::RecursiveStreams::<ChildCircuit<RootCircuit>>>::consolidate(traces);
                //.unwrap();

            let inferences_out = edb_union_idb
                //.distinct()
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
    pub fn consolidate_into_storage_layer(&mut self, storage_layer: &mut StorageLayer) {
        //self.dbsp_runtime.dump_profile("prof");
        self
            .fact_source
            .consolidate()
            .iter()
            .map(|((hashed_relation_symbol, encoded_fact), _, weight)| (hashed_relation_symbol, encoded_fact, weight))
            .for_each(|(hashed_relation_symbol, encoded_fact, weight)| {

                if weight.signum() > 0 {
                    storage_layer.push(&hashed_relation_symbol, encoded_fact);
                } else {
                    storage_layer.remove(&hashed_relation_symbol, &encoded_fact);
                }
            });
    }
}