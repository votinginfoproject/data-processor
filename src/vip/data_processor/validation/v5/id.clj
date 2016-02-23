(ns vip.data-processor.validation.v5.id
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

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
