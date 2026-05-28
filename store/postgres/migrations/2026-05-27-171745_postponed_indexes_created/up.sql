ALTER TABLE subgraphs.deployment
    ADD COLUMN postponed_indexes_created BOOLEAN NOT NULL DEFAULT FALSE;

-- Existing synced deployments already had their postponed indexes created
-- eagerly under the old code path; mark them done so we don't try again.
UPDATE subgraphs.deployment
    SET postponed_indexes_created = TRUE
    WHERE synced_at IS NOT NULL;
