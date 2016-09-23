(ns vip.data-processor.validation.v5.election
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.tools.logging :as log]))

(defn validate-one-election [{:keys [import-id] :as ctx}]
  (log/info "Validating one election")
  (let [result (korma/exec-raw
                (:conn postgres/xml-tree-values)
                ["SELECT COUNT(DISTINCT subltree(path, 0, 4)) AS election_count
                  FROM xml_tree_values
                  WHERE results_id = ?
                  AND path ~ 'VipObject.0.Election.*'" [import-id]]
                :results)
        election-count (-> result
                           first
                           :election_count)]
    (if (> election-count 1)
      (update-in ctx [:fatal :election "VipObject.0.Election" :count]
                 conj :more-than-one)
      ctx)))

(def validate-date
  (util/build-xml-tree-value-query-validator
   :fatal :election :missing :missing-date
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Date' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Election') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-state-id
  (util/build-xml-tree-value-query-validator
   :fatal :election :missing :missing-state-id
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'StateId' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Election') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
