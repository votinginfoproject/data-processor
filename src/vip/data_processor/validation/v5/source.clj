(ns vip.data-processor.validation.v5.source
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.fips :as fips]
            [vip.data-processor.validation.v5.util :as util]))

(defn validate-one-source [{:keys [import-id] :as ctx}]
  (let [result (korma/exec-raw
                (:conn postgres/xml-tree-values)
                ["SELECT COUNT(DISTINCT subltree(path, 0, 4)) AS source_count
                  FROM xml_tree_values
                  WHERE results_id = ?
                  AND path ~ 'VipObject.0.Source.*'" [import-id]]
                :results)
        source-count (-> result
                         first
                         :source_count)]
    (if (> source-count 1)
      (update-in ctx [:fatal :source "VipObject.0.Source" :count]
                 conj :more-than-one)
      ctx)))

(def validate-name
  (util/build-xml-tree-value-query-validator
   :fatal :source :missing :missing-name
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Name' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Source') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-date-time
  (util/build-xml-tree-value-query-validator
   :fatal :source :missing :missing-date-time
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'DateTime' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Source') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-vip-id
  (util/build-xml-tree-value-query-validator
   :fatal :source :missing :missing-name
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'VipId' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Source') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(defn validate-vip-id-valid-fips [{:keys [import-id] :as ctx}]
  (let [simple-path (postgres/path->ltree "VipObject.Source.VipId")
        vip-ids (korma/select postgres/xml-tree-values
                  (korma/where {:results_id import-id
                                :simple_path simple-path}))
        invalid-vip-ids (remove (comp fips/valid-fips? :value) vip-ids)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:critical :source (-> row :path .getValue) :invalid-fips]
                         conj (:value row)))
            ctx invalid-vip-ids)))
