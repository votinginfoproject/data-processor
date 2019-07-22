-- Index: xml_parent_id_idx

CREATE INDEX xml_parent_id_idx
  ON xml_tree_values
  USING btree
  (results_id, parent_with_id);

-- Index: xml_tree_values_idref_validation_idx

CREATE INDEX xml_tree_values_idref_validation_idx
  ON xml_tree_values
  USING btree
  (results_id, simple_path)
  WHERE simple_path ~ '*{2}.id'::lquery;

-- Index: xml_tree_values_no_duplicate_id_idx

CREATE INDEX xml_tree_values_no_duplicate_id_idx
  ON xml_tree_values
  USING btree
  (results_id DESC NULLS LAST, value COLLATE pg_catalog."default", path)
  WHERE path ~ 'VipObject.0.*.id'::lquery;

-- Index: xml_tree_values_result_path_idx

CREATE INDEX xml_tree_values_result_path_idx
  ON xml_tree_values
  USING btree
  (results_id, path);

-- Index: xml_tree_values_result_simple_path_idx

CREATE INDEX xml_tree_values_result_simple_path_idx
  ON xml_tree_values
  USING btree
  (results_id, simple_path);

-- Index: xml_tree_values_results_date_path_idx

CREATE INDEX xml_tree_values_results_date_path_idx
  ON xml_tree_values
  USING btree
  (results_id, simple_path)
  WHERE simple_path = 'VipObject.Election.Date'::ltree;

-- Index: xml_tree_values_results_election_type_path_idx

CREATE INDEX xml_tree_values_results_election_type_path_idx
  ON xml_tree_values
  USING btree
  (results_id, path)
  WHERE path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0'::lquery;

-- Index: xml_tree_values_results_element_type_idx

CREATE INDEX xml_tree_values_results_element_type_idx
  ON xml_tree_values
  USING btree
  (results_id, element_type(path) COLLATE pg_catalog."default");

-- Index: xml_tree_values_results_insert_counter_idx

CREATE INDEX xml_tree_values_results_insert_counter_idx
  ON xml_tree_values
  USING btree
  (results_id, insert_counter);

-- Index: xml_tree_values_results_state_path_idx

CREATE INDEX xml_tree_values_results_state_path_idx
  ON xml_tree_values
  USING btree
  (results_id, simple_path)
  WHERE simple_path = 'VipObject.State.Name'::ltree;

-- Index: xtv_i18n_paths_idx

CREATE INDEX xtv_i18n_paths_idx
  ON xml_tree_values
  USING btree
  (results_id, path)
  WHERE simple_path ~ 'VipObject.*.Text.language'::lquery AND value = 'en'::text;

-- Index: xtv_i18n_text_idx

CREATE INDEX xtv_i18n_text_idx
  ON xml_tree_values
  USING btree
  (results_id, path)
  WHERE simple_path ~ 'VipObject.*.Text'::lquery;

-- Index: xtv_localities_polling_locations_ids_idx

CREATE INDEX xtv_localities_polling_locations_ids_idx
  ON xml_tree_values
  USING btree
  (results_id, path, value COLLATE pg_catalog."default")
  WHERE path ~ 'VipObject.*.Locality.*.PollingLocationIds.*'::lquery;

-- Index: xtv_precincts_polling_locations_ids_idx

CREATE INDEX xtv_precincts_polling_locations_ids_idx
  ON xml_tree_values
  USING btree
  (results_id, path, value COLLATE pg_catalog."default")
  WHERE path ~ 'VipObject.*.Precinct.*.PollingLocationIds.*'::lquery;

-- Index: xtv_ss_precinct_idx

CREATE INDEX xtv_ss_precinct_idx
  ON xml_tree_values
  USING btree
  (results_id, simple_path, value COLLATE pg_catalog."default")
  WHERE simple_path = 'VipObject.StreetSegment.PrecinctId'::ltree;

-- Index: xml_tree_validations_result_path_idx

CREATE INDEX xml_tree_validations_result_path_idx
  ON xml_tree_validations
  USING btree
  (results_id, path);

-- Index: xml_tree_validations_result_type_idx

CREATE INDEX xml_tree_validations_result_type_idx
  ON xml_tree_validations
  USING btree
  (results_id, error_type COLLATE pg_catalog."default");

-- Index: xml_tree_validations_results_id_severity_idx

CREATE INDEX xml_tree_validations_results_id_severity_idx
  ON xml_tree_validations
  USING btree
  (results_id, severity COLLATE pg_catalog."default");
