-- 
create table __migrations (
    mod         varchar not null,
    name        varchar not null,
    PRIMARY KEY (mod, name),

    requires    varchar[],
    run_at      timestamptz default current_timestamp
);
