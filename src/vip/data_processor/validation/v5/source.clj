(ns vip.data-processor.validation.v5.source
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

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
