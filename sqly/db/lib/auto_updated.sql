/*
To make the "updated" column timestamp auto-update when the row is updated on tablename, 
execute "select auto_updated(tablename);", which creates a trigger that will set
the "updated" column to the current_timestamp if it has not been changed. (The last
condition means that if the app user assigns a value other than the old value, 
that assignment will be respected.)
*/

CREATE OR REPLACE FUNCTION auto_updated(_tbl regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE format('CREATE TRIGGER auto_updated BEFORE UPDATE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE auto_updated_trfn()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION auto_updated_trfn() RETURNS trigger AS $$
BEGIN
    IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.updated IS NOT DISTINCT FROM OLD.updated
    ) THEN
        NEW.updated := current_timestamp;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
