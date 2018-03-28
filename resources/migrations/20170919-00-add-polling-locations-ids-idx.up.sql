DROP INDEX IF EXISTS xtv_localities_polling_locations_ids_idx;

CREATE INDEX xtv_localities_polling_locations_ids_idx
ON xml_tree_values (results_id, path, value)
WHERE path ~ 'VipObject.*.Locality.*.PollingLocationIds.*';

DROP INDEX IF EXISTS xtv_precincts_polling_locations_ids_idx;

CREATE INDEX xtv_precincts_polling_locations_ids_idx
ON xml_tree_values (results_id, path, value)
WHERE path ~ 'VipObject.*.Precinct.*.PollingLocationIds.*';

