/* 
## Automatic random ids 
`auto_random_id(TABLENAME)` -- sets up TABLENAME to have an automatic random ids.
*/
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION gen_random_id(length integer) RETURNS VARCHAR AS $$
DECLARE
    chars text[] := '{0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z}';
    new_id text := '';
    index integer := 0;
BEGIN
    IF length < 0 THEN
        RAISE EXCEPTION 'Given length cannot be less than 0';
    END IF;
    -- first character must be a letter, not a number, so it can be used as an XML id.
    new_id := new_id || chars[11+random()*(array_length(chars, 1)-11)];
    -- rest of characters can be any of chars
    for index in 1..length-1 loop
        new_id := new_id || chars[1+random()*(array_length(chars, 1)-1)];
    END loop;
    return new_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION auto_random_id(_tbl regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE format('CREATE TRIGGER set_random_id BEFORE INSERT ON %s
                    FOR EACH ROW EXECUTE PROCEDURE auto_random_id_trfn()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION auto_random_id_trfn() RETURNS trigger AS $$
BEGIN
    -- Only assign NEW.id if it isn't given -- to preserve existing ids! (e.g., load by insert)
    IF NEW.id IS null THEN
        NEW.id := gen_random_id(32);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
