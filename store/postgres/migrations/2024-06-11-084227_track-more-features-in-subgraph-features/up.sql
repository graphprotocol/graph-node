ALTER TABLE subgraphs.subgraph_features
  ADD COLUMN IF NOT EXISTS has_declared_calls BOOLEAN NOT NULL DEFAULT FALSE;