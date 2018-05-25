extern crate assert_cli;

static WITHOUT_ARGS_OUTPUT: &'static str =
    "error: The following required arguments were not provided:
    --db <URL>

USAGE:
    thegraph-local-node --db <URL>

For more information try --help
";

#[cfg(test)]
mod integration {
    use assert_cli;
    use WITHOUT_ARGS_OUTPUT;

    #[test]
    fn starting_local_node_without_args() {
        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .is(WITHOUT_ARGS_OUTPUT)
            .unwrap();
    }
}
