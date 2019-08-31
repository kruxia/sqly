-- 
create table __migrations (
    name        varchar not null PRIMARY KEY,
    requires    varchar[],
    created     timestamp_with_timezone default current_timestamp,
    run_at      timestamp_with_timezone
);
