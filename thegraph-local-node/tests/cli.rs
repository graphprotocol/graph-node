extern crate assert_cli;

use assert_cli;

#[test]
fn local_node_fails_to_start_without_postgres_url() {
    assert_cli::Assert::main_binary()
        .fails()
        .and()
        .stderr()
        .contains("error: The following required arguments were not provided:")
        .unwrap();
}
