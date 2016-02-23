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

(def missing-id-query-string
  "A close reading of the 5.0 and 5.1 spec reveals that only direct
  children of the VipObject element require an id attribute and that
  *every* direct child of the VipObject requires an id
  attribute. Thus, this query generates every id path that should
  exist (in the first subquery), and then left joins against paths
  that do exist to find the ones that are missing."
  "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'id' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND nlevel(subltree(path, 0, 4)) = 4) xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = xtv2.path
    WHERE xtv2.path IS NULL")

(defn missing-id-query
  "Generates a query vector for korma's exec-raw to find missing ids."
  [import-id]
  [missing-id-query-string [import-id import-id]])

(defn missing-ids
  "For an import, find all the id paths that should exist but don't."
  [import-id]
  (korma/exec-raw
   (:conn postgres/xml-tree-values)
   (missing-id-query import-id)
   :results))

(defn validate-no-missing-ids
  [{:keys [import-id] :as ctx}]
  (->> (missing-ids import-id)
       (map :path)
       (reduce (fn [ctx path]
                 (update-in ctx [:fatal :id (.getValue path) :missing]
                            conj :missing-id))
               ctx)))
