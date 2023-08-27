ALTER TABLE objects
    ADD COLUMN fields TEXT DEFAULT NULL;

ALTER TABLE objects_history
    ADD COLUMN fields TEXT DEFAULT NULL;

ALTER TABLE events
    ADD COLUMN parsed_json TEXT DEFAULT NULL;

-- CREATE OR REPLACE FUNCTION objects_modified_func() RETURNS TRIGGER AS
-- $body$
-- BEGIN
--     IF (TG_OP = 'INSERT') THEN
--         INSERT INTO objects_history
--         VALUES (NEW.epoch, NEW.checkpoint, NEW.object_id, NEW.version, NEW.object_digest, NEW.owner_type,
--                 NEW.owner_address, NULL, NULL,
--                 NEW.initial_shared_version,
--                 NEW.previous_transaction, NEW.object_type, NEW.object_status, NEW.has_public_transfer,
--                 NEW.storage_rebate, NEW.bcs, NEW.fields);
--         RETURN NEW;
--     ELSEIF (TG_OP = 'UPDATE') THEN
--         INSERT INTO objects_history
--         VALUES (NEW.epoch, NEW.checkpoint, NEW.object_id, NEW.version, NEW.object_digest, NEW.owner_type,
--                 NEW.owner_address, OLD.owner_type, OLD.owner_address,
--                 NEW.initial_shared_version,
--                 NEW.previous_transaction, NEW.object_type, NEW.object_status, NEW.has_public_transfer,
--                 NEW.storage_rebate, NEW.bcs, NEW.fields);
--         -- MUSTFIX(gegaowp): we cannot update checkpoint in-place, b/c checkpoint is a partition key,
--         -- we need to prune old data in this partition periodically, like pruning old epochs upon new epoch.
--         RETURN NEW;
--     ELSIF (TG_OP = 'DELETE') THEN
--         -- object deleted from the main table, archive the history for that object
--         DELETE FROM objects_history WHERE object_id = old.object_id;
--         RETURN OLD;
--     ELSE
--         RAISE WARNING '[OBJECTS_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,NOW();
--         RETURN NULL;
--     END IF;

-- EXCEPTION
--     WHEN data_exception THEN
--         RAISE WARNING '[OBJECTS_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
--         RETURN NULL;
--     WHEN unique_violation THEN
--         RAISE WARNING '[OBJECTS_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
--         RETURN NULL;
--     WHEN OTHERS THEN
--         RAISE WARNING '[OBJECTS_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
--         RETURN NULL;
-- END;
-- $body$
--     LANGUAGE plpgsql;