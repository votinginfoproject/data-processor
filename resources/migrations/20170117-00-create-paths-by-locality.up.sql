create table v5_dashboard.paths_by_locality (
    results_id integer references results (id) not null,
    locality_id text,
    paths ltree[] not null);
