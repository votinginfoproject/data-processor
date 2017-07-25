DROP INDEX IF EXISTS xtv_i18n_text_idx;

CREATE INDEX xtv_i18n_text_idx
ON xml_tree_values (results_id, path)
WHERE simple_path ~ 'VipObject.*.Text';
