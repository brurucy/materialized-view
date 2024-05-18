#[cfg(test)]
mod tests {
    use datalog_rule_macro::program;
    use datalog_rule_macro::rule;
    use datalog_syntax::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_program() {
        let expected_program = Program::from(vec![
            rule! { tc(?x, ?y) <- [e(?x, ?y)] },
            rule! { tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)] },
        ]);
        let actual_program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)]
        };

        assert_eq!(expected_program, actual_program);
    }
}
