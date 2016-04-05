(ns vip.data-processor.validation.v5.retention-contest
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-candidate-ids
  (util/build-xml-tree-value-query-validator
   :errors :retention-contests :missing :missing-candidate-id
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'CandidateId' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.RetentionContest') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
