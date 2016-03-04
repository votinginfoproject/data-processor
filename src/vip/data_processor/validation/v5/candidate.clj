(ns vip.data-processor.validation.v5.candidate
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]))

(defn valid-pre-election-status? [status]
  (#{"filed" "qualified" "withdrawn" "write-in"} (:value status)))

(defn valid-post-election-status? [status]
  (#{"advanced-to-runoff" "projected-winner" "winner" "withdrawn"}
   (:value status)))

(defn validate-pre-election-statuses [{:keys [import-id] :as ctx}]
  (let [statuses (korma/select postgres/xml-tree-values
                               (korma/where {:results_id import-id})
                               (korma/where
                                (postgres/ltree-match
                                 postgres/xml-tree-values :path
                                 "VipObject.0.Candidate.*{1}.PreElectionStatus.*{1}")))
        invalid-statuses (remove valid-pre-election-status? statuses)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :candidates (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-statuses)))

(defn validate-post-election-statuses [{:keys [import-id] :as ctx}]
  (let [statuses (korma/select postgres/xml-tree-values
                               (korma/where {:results_id import-id})
                               (korma/where
                                (postgres/ltree-match
                                 postgres/xml-tree-values :path
                                 "VipObject.0.Candidate.*{1}.PostElectionStatus.*{1}")))
        invalid-statuses (remove valid-post-election-status? statuses)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :candidates (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-statuses)))

(def missing-ballot-name-query-string
  "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'BallotName' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 4) ~ 'VipObject.0.Candidate.*{1}'
          AND nlevel(subltree(path, 0, 4)) = 4) xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL")

(defn missing-ballot-name-query
  "Generates a query vector for korma's exec-raw to find missing BallotNames."
  [import-id]
  [missing-ballot-name-query-string [import-id import-id]])

(defn missing-ballot-names
  "For an import, find all the BallotName paths that should exist but don't."
  [import-id]
  (korma/exec-raw
   (:conn postgres/xml-tree-values)
   (missing-ballot-name-query import-id)
   :results))

(defn validate-no-missing-ballot-names [{:keys [import-id] :as ctx}]
  (->> (missing-ballot-names import-id)
       (map :path)
       (reduce (fn [ctx path]
                 (update-in ctx [:errors :candidates (.getValue path) :missing]
                            conj :missing-ballot-name))
               ctx)))
