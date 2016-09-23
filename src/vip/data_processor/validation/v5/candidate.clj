(ns vip.data-processor.validation.v5.candidate
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.tools.logging :as log]))

(defn valid-pre-election-status? [status]
  (#{"filed" "qualified" "withdrawn" "write-in"} (:value status)))

(defn valid-post-election-status? [status]
  (#{"advanced-to-runoff" "projected-winner" "winner" "withdrawn"}
   (:value status)))

(defn validate-pre-election-statuses [{:keys [import-id] :as ctx}]
  (log/info "Validating PreElectionStatus elements")
  (let [statuses (korma/select postgres/xml-tree-values
                   (korma/where {:results_id import-id
                                 :simple_path (postgres/path->ltree "VipObject.Candidate.PreElectionStatus")}))
        invalid-statuses (remove valid-pre-election-status? statuses)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :candidates (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-statuses)))

(defn validate-post-election-statuses [{:keys [import-id] :as ctx}]
  (log/info "Validating PostElectionStatus elements")
  (let [statuses (korma/select postgres/xml-tree-values
                   (korma/where {:results_id import-id
                                 :simple_path (postgres/path->ltree "VipObject.Candidate.PostElectionStatus")}))
        invalid-statuses (remove valid-post-election-status? statuses)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :candidates (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-statuses)))

(def validate-no-missing-ballot-names
  (util/build-xml-tree-value-query-validator
   :errors :candidates :missing :missing-ballot-name
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'BallotName' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.Candidate') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
