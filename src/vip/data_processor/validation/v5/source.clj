(ns vip.data-processor.validation.v5.source
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [clojure.string :as str]))

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

(defn validate-source-name [{:keys [import-id] :as ctx}]
  (let [path "VipObject.0.Source.*{1}.Name.*{1}"
        source-name (-> (korma/select postgres/xml-tree-values
                                      (korma/where {:results_id import-id})
                                      (korma/where
                                       (postgres/ltree-match
                                        postgres/xml-tree-values :path path)))
                        first
                        :value)]
    (if (str/blank? source-name)
      (update-in ctx [:fatal :source path :missing]
                 conj :missing-name)
      ctx)))
