(ns vip.data-processor.validation.v5.candidate
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]))

(defn valid-post-election-status? [status]
  (#{"advanced-to-runoff" "projected-winner" "winner" "withdrawn"}
   (:value status)))

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
