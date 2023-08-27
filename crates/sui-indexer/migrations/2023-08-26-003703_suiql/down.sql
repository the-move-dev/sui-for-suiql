ALTER TABLE objects
    DROP COLUMN fields CASCADE;

ALTER TABLE objects_history
    DROP COLUMN fields CASCADE;

ALTER TABLE events
    DROP COLUMN fields CASCADE;
