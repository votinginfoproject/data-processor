(ns vip.data-processor.validation.v5.external-identifiers
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-types
  (util/build-xml-tree-value-query-validator
   :errors :external-identifiers :missing :missing-type
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 8) || 'Type' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 8) ~ 'VipObject.0.*{4}.ExternalIdentifier.*{1}') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 9)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-no-missing-values
  (util/build-xml-tree-value-query-validator
   :errors :external-identifiers :missing :missing-value
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 8) || 'Value' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 8) ~ 'VipObject.0.*{4}.ExternalIdentifier.*{1}') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 9)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
