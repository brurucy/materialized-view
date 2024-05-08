use clap::{Arg, Command};
use chibi_datalog::evaluation::benchmark_maker::into_benchmark;
use chibi_datalog::evaluation::evaluator::{DyreOWL2RL, DyreRDFS, DyreTC, Evaluator};
use chibi_datalog::evaluation::loader::{DataLoader, DenseEdges, LUBMOWL2RL, RDF, SparseEdges};
use crate::Dataset::{Dense, OWL2RL, RDFS, Sparse};

pub enum Dataset {
    Dense,
    Sparse,
    RDFS,
    OWL2RL
}

fn main() {
    let matches = Command::new("dyre-bencher")
        .version("0.1.0")
        .about(
            "Benches the time taken to reason",
        )
        .arg(
            Arg::new("DATASET")
                .help("dense, sparse, rdfs or owl2rl")
                .required(true)
                .index(1),
        )
        .get_matches();
    let dataset: Dataset = match matches.value_of("DATASET").unwrap() {
        "dense" => Dense,
        "sparse" => Sparse,
        "rdfs" => RDFS,
        "owl2rl" => OWL2RL,
        other => panic!("unknown dataset: {}", other),
    };

    let mat_mct_sizes = vec![0, 500, 750, 900, 990];
    match dataset {
        Dense => {
            let dense_edges = DenseEdges::new().load();

            for mat_mct_size in &mat_mct_sizes {
                let mut dyre_tc_dense = DyreTC::new();
                let dense_updates = into_benchmark(dense_edges.clone(), *mat_mct_size);
                for (idx, update) in dense_updates.iter().enumerate() {
                    let elapsed_dense = dyre_tc_dense.update(&update);
                    let op = if idx == 0 { "mat" } else if idx == 1 { "add" } else { "del" };

                    println!("Elapsed Dense {} - {} - {} ms - {} tuples", op, mat_mct_size, elapsed_dense, dyre_tc_dense.triple_count());
                }
                println!("\n")
            }
        },
        Sparse => {
            let sparse_edges = SparseEdges::new().load();

            for mat_mct_size in &mat_mct_sizes {
                let mut dyre_tc_sparse = DyreTC::new();
                let sparse_updates = into_benchmark(sparse_edges.clone(), *mat_mct_size);
                for (idx, update) in sparse_updates.iter().enumerate() {
                    let elapsed_sparse = dyre_tc_sparse.update(&update);
                    let op = if idx == 0 { "mat" } else if idx == 1 { "add" } else { "del" };

                    println!("Elapsed Sparse {} - {} - {} ms - {} tuples", op, mat_mct_size, elapsed_sparse, dyre_tc_sparse.triple_count());
                }
                println!("\n")
            }

        },
        RDFS => {
            let rdfs = RDF::new().load();

            for mat_mct_size in &mat_mct_sizes {
                let mut dyre_rdfs = DyreRDFS::new();
                let rdfs_updates = into_benchmark(rdfs.clone(), *mat_mct_size);
                for (idx, update) in rdfs_updates.iter().enumerate() {
                    let elapsed_rdfs = dyre_rdfs.update(&update);
                    let op = if idx == 0 { "mat" } else if idx == 1 { "add" } else { "del" };

                    println!("Elapsed RDFS {} - {} - {} ms - {} tuples", op, mat_mct_size, elapsed_rdfs, dyre_rdfs.triple_count());
                }
                println!("\n")
            }
        },
        OWL2RL => {
            let owl2rl = LUBMOWL2RL::new().load();

            for mat_mct_size in &mat_mct_sizes {
                let mut dyre_owl2rl = DyreOWL2RL::new();
                let owl2rl_updates = into_benchmark(owl2rl.clone(), *mat_mct_size);
                for (idx, update) in owl2rl_updates.iter().enumerate() {
                    let elapsed_owl2rl = dyre_owl2rl.update(&update);
                    let op = if idx == 0 { "mat" } else if idx == 1 { "add" } else { "del" };

                    println!("Elapsed OWL2RL {} - {} - {} ms - {} tuples", op, mat_mct_size, elapsed_owl2rl, dyre_owl2rl.triple_count());
                }
                println!("\n")
            }
        },
    }
}