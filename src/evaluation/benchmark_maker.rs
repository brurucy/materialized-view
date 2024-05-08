use crate::evaluation::evaluator::Diff;

pub fn into_benchmark(diffs: Vec<Diff>, mat_mct_size: usize) -> [Vec<Diff>; 3] {
    let last_mat_idx = (diffs.len() as f32 * (mat_mct_size as f32 / 1000.0)) as usize;

    let mut out: [Vec<Diff>; 3] = Default::default();
    diffs
        .into_iter()
        .enumerate()
        .for_each(|(idx, diff)| {
            let mut diff_with_weight = diff;

            if idx < last_mat_idx {
                diff_with_weight.2 = 1;
                out[0].push(diff_with_weight)
            } else {
                diff_with_weight.2 = 1;
                let mut diff_with_weight_negative = diff_with_weight.clone();
                out[1].push(diff_with_weight);

                diff_with_weight_negative.2 = -1;
                out[2].push(diff_with_weight_negative);
            }
        });

    out
}

#[cfg(test)]
mod tests {
    use crate::evaluation::benchmark_maker::into_benchmark;

    #[test]
    fn test_into_benchmark() {
        let diffs = (0..1000).map(|count| (None, vec![count as usize], 0)).collect();
        let benchmark = into_benchmark(diffs, 998);

        assert_eq!(benchmark[0].len(), 998);
        assert_eq!(benchmark[1].len(), 2);
        assert_eq!(benchmark[2].len(), 2);
    }
}