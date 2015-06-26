CREATE TABLE validations (results_id INTEGER,
                          severity character varying(255),
                          scope character varying(255),
                          identifier BIGINT,
                          error_type character varying(255),
                          error_data text);

CREATE INDEX validations_result_scope_id_idx ON validations (results_id, scope, identifier);
CREATE INDEX validations_result_scope_type_idx ON validations (results_id, scope, error_type);
