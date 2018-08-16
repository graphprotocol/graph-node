/**************************************************************
* NOTIFY ENTITY ADDED
*
* Emits an entity added notification over the "entity_changes"
* notification channel.
**************************************************************/

CREATE OR REPLACE FUNCTION notify_entity_added()
    RETURNS trigger AS
$$
DECLARE
BEGIN
    PERFORM pg_notify('entity_changes', json_build_object(
      'subgraph', NEW.subgraph,
      'entity', NEW.entity,
      'id', NEW.id,
      'data', NEW.data,
      'operation', 'add'
    )::text);
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

/**************************************************************
 * NOTIFY ENTITY UPDATED
 *
 * Emits an entity updated notification over the "entity_changes"
 * notification channel.
 **************************************************************/

CREATE OR REPLACE FUNCTION notify_entity_updated()
    RETURNS trigger AS
$$
DECLARE
BEGIN
    PERFORM pg_notify('entity_changes', json_build_object(
        'subgraph', NEW.subgraph,
        'entity', NEW.entity,
        'id', NEW.id,
        'data', NEW.data,
        'operation', 'update'
    )::text);
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

/**************************************************************
 * NOTIFY ENTITY REMOVED
 *
 * Emits an entity removed notification over the "entity_changes"
 * notification channel.
 **************************************************************/

CREATE OR REPLACE FUNCTION notify_entity_removed()
    RETURNS trigger AS
$$
DECLARE
BEGIN
    PERFORM pg_notify('entity_changes', json_build_object(
        'subgraph', OLD.subgraph,
        'entity', OLD.entity,
        'id', OLD.id,
        'data', OLD.data,
        'operation', 'remove'
    )::text);
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

/**************************************************************
 * CREATE TRIGGERS
 **************************************************************/

CREATE TRIGGER entity_added
AFTER INSERT
ON entities
FOR EACH ROW
EXECUTE PROCEDURE notify_entity_added();

CREATE TRIGGER entity_updated
AFTER UPDATE
ON entities
FOR EACH ROW
EXECUTE PROCEDURE notify_entity_updated();

CREATE TRIGGER entity_removed
AFTER DELETE
ON entities
FOR EACH ROW
EXECUTE PROCEDURE notify_entity_removed();
