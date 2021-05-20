CREATE TABLE IF NOT EXISTS subgraphs.graph_node_versions (
    id serial PRIMARY KEY,
    git_commit_hash text NOT NULL,
    git_repository_dirty boolean NOT NULL,
    crate_version text NOT NULL,
    major integer NOT NULL,
    minor integer NOT NULL,
    patch integer NOT NULL,
    pre_release text NOT NULL,
    rustc_version text NOT NULL,
    rustc_host text NOT NULL,
    rustc_channel text NOT NULL,
    CONSTRAINT unique_graph_node_versions UNIQUE (git_commit_hash, git_repository_dirty, crate_version, major, minor, patch, pre_release, rustc_version, rustc_host, rustc_channel)
);

ALTER TABLE subgraphs.subgraph_manifest
    ADD COLUMN graph_node_version_id integer,
    ADD CONSTRAINT graph_node_versions_fk FOREIGN KEY (graph_node_version_id) REFERENCES subgraphs.graph_node_versions (id);
