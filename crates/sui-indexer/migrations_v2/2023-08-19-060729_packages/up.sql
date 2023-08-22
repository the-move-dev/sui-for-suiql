DO
$$
    BEGIN
        CREATE TYPE bcs_bytes AS
        (
            name TEXT,
            data bytea
        );
    EXCEPTION
        WHEN duplicate_object THEN
            -- Type already exists, do nothing
            NULL;
    END
$$;


CREATE TABLE packages 
(
    package_id                   bytea      PRIMARY KEY,
    -- version                      bigint     NOT NULL,
    -- initial_package_id           bytea      NOT NULL,
    -- serialized_move_package      bytea      NOT NULL
    modules       bcs_bytes[] NOT NULL
);
