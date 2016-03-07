(ns vip.data-processor.validation.v5.id
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]))

(defn duplicate-ids [import-id]
  (korma/select [postgres/xml-tree-values :first]
    (korma/fields :first.value :first.path)
    (korma/where {:first.results_id import-id :second.results_id import-id})
    (korma/where (postgres/ltree-match :first :path "VipObject.0.*.id"))
    (korma/where (postgres/ltree-match :second :path "VipObject.0.*.id"))
    (korma/join :inner [postgres/xml-tree-values :second]
                (and
                 (= :first.value :second.value)
                 (not= :first.path :second.path)))))

(defn validate-unique-ids
  [{:keys [import-id] :as ctx}]
  (let [duplicate-ids (duplicate-ids import-id)]
    (reduce (fn [ctx row]
              (let [id (:value row)
                    path (-> row :path .getValue)]
                (update-in ctx [:fatal :id path :duplicates] conj id)))
            ctx duplicate-ids)))

(def validate-no-missing-ids
  (util/build-xml-tree-value-query-validator
   :fatal :id :missing :missing-id
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'id' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND nlevel(subltree(path, 0, 4)) = 4) xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = xtv2.path
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
