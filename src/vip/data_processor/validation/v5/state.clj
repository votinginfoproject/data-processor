(ns vip.data-processor.validation.v5.state
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/build-xml-tree-value-query-validator
   :fatal :states :missing :missing-name
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Name' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 4) ~ 'VipObject.0.State.*{1}') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
