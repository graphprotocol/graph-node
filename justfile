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

new-migration NAME:
    diesel migration generate {{ NAME }} --migration-dir store/postgres/migrations/
