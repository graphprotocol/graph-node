ALTER TABLE subgraphs.subgraph_features
  DROP COLUMN IF EXISTS has_declared_calls,
  DROP COLUMN IF EXISTS has_bytes_as_ids,
  DROP COLUMN IF EXISTS has_aggregations,
  DROP COLUMN IF EXISTS immutable_entities;