use lasso::{Key, Rodeo};
use phf::phf_map;
use crate::evaluation::evaluator::Diff;

pub trait DataLoader {
    fn new() -> Self;
    fn load(&self) -> Vec<Diff>;
}

pub struct DenseEdges<'a> {
    data: &'a str
}

impl<'a> DataLoader for DenseEdges<'a> {
    fn new() -> Self {
        let data = include_str!("../../data/graph1000.txt");

        Self { data }
    }

    fn load(&self) -> Vec<Diff> {
        self
            .data
            .lines()
            .into_iter()
            .map(|line| {
                let triple: Vec<_> = line.split(" ").collect();
                let from: usize = triple[0].parse().unwrap();
                let to: usize = triple[1].parse().unwrap();

                (None, vec![from, to], 0)
            })
            .collect()
    }
}

pub struct SparseEdges<'a> {
    data: &'a str
}

impl<'a> DataLoader for SparseEdges<'a> {
    fn new() -> Self {
        let data = include_str!("../../data/graph10000.txt");

        Self { data }
    }

    fn load(&self) -> Vec<Diff> {
        self
            .data
            .lines()
            .into_iter()
            .map(|line| {
                let triple: Vec<_> = line.split(" ").collect();
                let from: usize = triple[0].parse().unwrap();
                let to: usize = triple[1].parse().unwrap();

                (None, vec![from, to], 0)
            })
            .collect()
    }
}

pub struct RDF<'a> {
    data: &'a str
}

impl<'a> DataLoader for RDF<'a> {
    fn new() -> Self {
        let data = include_str!("../../data/lubm1.nt");

        Self { data }
    }

    fn load(&self) -> Vec<Diff> {
        let mut rodeo = Rodeo::default();
        let r#type: &'static str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        let sub_class_of: &'static str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
        let sub_property_of: &'static str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
        let domain: &'static str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
        let range: &'static str = "<http://www.w3.org/2000/01/rdf-schema#range>";
        let property: &'static str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>";
        rodeo.get_or_intern(r#type);
        rodeo.get_or_intern(sub_class_of);
        rodeo.get_or_intern(sub_property_of);
        rodeo.get_or_intern(domain);
        rodeo.get_or_intern(range);
        rodeo.get_or_intern(property);

        self
            .data
            .lines()
            .into_iter()
            .filter_map(|line| {
                if !line.contains("genid") {
                    let triple: Vec<_> = line
                        .split_whitespace()
                        .map(|resource| resource.trim())
                        .collect();
                    let s = rodeo.get_or_intern(triple[0]).into_usize();
                    let p = rodeo.get_or_intern(triple[1]).into_usize();
                    let o = rodeo.get_or_intern(triple[2]).into_usize();

                    return Some((None, vec![s, p, o], 0))
                };

                None
            })
            .collect()
    }
}

static OWL: phf::Map<&'static str, &'static str> = phf_map! {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" => "rdf:type",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest" => "rdf:rest",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first" =>"rdf:first",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil" =>"rdf:nil",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" =>"rdf:Property",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf" =>"rdfs:subClassOf",
    "http://www.w3.org/2000/01/rdf-schema#subPropertyOf" =>"rdfs:subPropertyOf",
    "http://www.w3.org/2000/01/rdf-schema#domain" =>"rdfs:domain",
    "http://www.w3.org/2000/01/rdf-schema#range" =>"rdfs:range",
    "http://www.w3.org/2000/01/rdf-schema#comment" =>"rdfs:comment",
    "http://www.w3.org/2000/01/rdf-schema#label" =>"rdfs:label",
    "http://www.w3.org/2000/01/rdf-schema#Literal" =>"rdfs:Literal",
    "http://www.w3.org/2002/07/owl#TransitiveProperty" =>"owl:TransitiveProperty",
    "http://www.w3.org/2002/07/owl#inverseOf" =>"owl:inverseOf",
    "http://www.w3.org/2002/07/owl#Thing" =>"owl:Thing",
    "http://www.w3.org/2002/07/owl#maxQualifiedCardinality" =>"owl:maxQualifiedCardinality",
    "http://www.w3.org/2002/07/owl#someValuesFrom" =>"owl:someValuesFrom",
    "http://www.w3.org/2002/07/owl#equivalentClass" =>"owl:equivalentClass",
    "http://www.w3.org/2002/07/owl#intersectionOf" =>"owl:intersectionOf",
    "http://www.w3.org/2002/07/owl#members" =>"owl:members",
    "http://www.w3.org/2002/07/owl#equivalentProperty" =>"owl:equivalentProperty",
    "http://www.w3.org/2002/07/owl#onProperty" =>"owl:onProperty",
    "http://www.w3.org/2002/07/owl#propertyChainAxiom" =>"owl:propertyChainAxiom",
    "http://www.w3.org/2002/07/owl#disjointWith" =>"owl:disjointWith",
    "http://www.w3.org/2002/07/owl#propertyDisjointWith" =>"owl:propertyDisjointWith",
    "http://www.w3.org/2002/07/owl#unionOf" =>"owl:unionOf",
    "http://www.w3.org/2002/07/owl#hasKey" =>"owl:hasKey",
    "http://www.w3.org/2002/07/owl#allValuesFrom" =>"owl:allValuesFrom",
    "http://www.w3.org/2002/07/owl#complementOf" =>"owl:complementOf",
    "http://www.w3.org/2002/07/owl#onClass" =>"owl:onClass",
    "http://www.w3.org/2002/07/owl#distinctMembers" =>"owl:distinctMembers",
    "http://www.w3.org/2002/07/owl#FunctionalProperty" =>"owl:FunctionalProperty",
    "http://www.w3.org/2002/07/owl#NamedIndividual" =>"owl:NamedIndividual",
    "http://www.w3.org/2002/07/owl#ObjectProperty" =>"owl:ObjectProperty",
    "http://www.w3.org/2002/07/owl#Class" =>"owl:Class",
    "http://www.w3.org/2002/07/owl#AllDisjointClasses" =>"owl:AllDisjointClasses",
    "http://www.w3.org/2002/07/owl#Restriction" =>"owl:Restriction",
    "http://www.w3.org/2002/07/owl#DatatypeProperty" =>"owl:DatatypeProperty",
    "http://www.w3.org/2002/07/owl#Ontology" =>"owl:Ontology",
    "http://www.w3.org/2002/07/owl#AsymmetricProperty" =>"owl:AsymmetricProperty",
    "http://www.w3.org/2002/07/owl#SymmetricProperty" =>"owl:SymmetricProperty",
    "http://www.w3.org/2002/07/owl#IrreflexiveProperty" =>"owl:IrreflexiveProperty",
    "http://www.w3.org/2002/07/owl#AllDifferent" =>"owl:AllDifferent",
    "http://www.w3.org/2002/07/owl#InverseFunctionalProperty" =>"owl:InverseFunctionalProperty",
    "http://www.w3.org/2002/07/owl#sameAs" =>"owl:sameAs",
    "http://www.w3.org/2002/07/owl#hasValue" =>"owl:hasValue",
    "http://www.w3.org/2002/07/owl#Nothing" =>"owl:Nothing",
    "http://www.w3.org/2002/07/owl#oneOf" =>"owl:oneOf",
};

pub struct LUBMOWL2RL<'a> {
    data: &'a str
}

impl<'a> DataLoader for LUBMOWL2RL<'a> {
    fn new() -> Self {
        let data = include_str!("../../data/lubm1.nt");

        Self { data }
    }

    fn load(&self) -> Vec<Diff> {
        let mut rodeo = Rodeo::default();
        self
            .data
            .lines()
            .into_iter()
            .filter_map(|line| {
                if !line.contains("genid") {
                    let split_line: String = line
                        .replace("<", "")
                        .replace(">", "");

                    let clean_split_line: Vec<_> = split_line
                        .split_whitespace()
                        .filter(|c| !c.is_empty() && !(*c == "."))
                        .collect();

                    let mut digit_one = clean_split_line[0];
                    let mut digit_two = clean_split_line[1];
                    let mut digit_three = clean_split_line[2..].join(" ");

                    if let Some(alias) = OWL.get(&digit_one) {
                        digit_one = alias;
                    }
                    if let Some(alias) = OWL.get(&digit_two) {
                        digit_two = alias;
                    }
                    if let Some(alias) = OWL.get(&digit_three) {
                        digit_three = alias.to_string();
                    }

                    let mut diff = None;
                    let prefix = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
                    match digit_two {
                        "rdf:type" => {
                            let mut sym = digit_three.to_string();
                            if let Some(prefix_striped_sym) = sym.strip_prefix(prefix) {
                                sym = prefix_striped_sym.to_string()
                            }

                            diff = Some((Some(sym), vec![rodeo.get_or_intern(digit_one).into_usize()], 0));
                        }
                        possibly_property => {
                            if possibly_property.contains(prefix) {
                                if let Some(property) = possibly_property.strip_prefix(prefix) {
                                    let sym = property.to_string();
                                    let digit_three = digit_three.to_string();

                                    diff = Some((Some(sym), vec![
                                        rodeo.get_or_intern(digit_one).into_usize(),
                                        rodeo.get_or_intern(digit_three).into_usize()
                                    ], 0));
                                };
                            }
                        }
                    };

                    return diff
                };

                None
            })
            .collect()
    }
}