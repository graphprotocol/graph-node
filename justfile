DATABASE_TEST_VAR_NAME := "THEGRAPH_STORE_POSTGRES_DIESEL_URL"
DATABASE_URL := "postgresql://graph-node:let-me-in@localhost:5432/graph-node"


help:
    @just -l

local-deps-up *ARGS:
    docker compose -f docker/docker-compose.yml up ipfs postgres {{ ARGS }}

local-deps-down:
    docker compose -f docker/docker-compose.yml down

test-deps-up *ARGS:
    docker compose -f tests/docker-compose.yml up {{ ARGS }}

test-deps-down:
    docker compose -f tests/docker-compose.yml down

# Requires local-deps, see local-deps-up
test *ARGS:
    just _run_in_bash cargo test --workspace --exclude graph-tests -- --nocapture {{ ARGS }}

runner-test *ARGS:
    just _run_in_bash cargo test -p graph-tests --test runner_tests -- --nocapture {{ ARGS }}

# Requires test-deps to be running, see test-deps-up
it-test *ARGS:
    just _run_in_bash cargo test --test integration_tests -- --nocapture {{ ARGS }}

local-rm-db:
    rm -r docker/data/postgres

new-migration NAME:
    diesel migration generate {{ NAME }} --migration-dir store/postgres/migrations/

_run_in_bash *CMD:
    #!/usr/bin/env bash
    export {{ DATABASE_TEST_VAR_NAME }}={{ DATABASE_URL }}
    {{ CMD }}
