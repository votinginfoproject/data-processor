create extension btree_gist;

create table v5_street_segment_validations (
    results_id bigint NOT NULL,
    path ltree,
    severity character varying(255) COLLATE pg_catalog."default",
    scope character varying(255) COLLATE pg_catalog."default",
    error_type character varying(255) COLLATE pg_catalog."default",
    error_data text COLLATE pg_catalog."default",
    parent_element_id text COLLATE pg_catalog."default",
    CONSTRAINT xml_tree_validations_results_id_fkey FOREIGN KEY (results_id)
        REFERENCES public.results (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

create index v5_street_segment_paths on v5_street_segment_validations using gist (results_id, path);
