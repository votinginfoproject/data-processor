(ns vip.data-processor.validation.v5.election-administration
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.tools.logging :as log]))

(def validate-no-missing-departments
  (util/build-xml-tree-value-query-validator
   :errors :election-administration :missing :missing-department
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Department' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 2) = 'VipObject.ElectionAdministration') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def voter-service-types
  #{"absentee-ballots" "overseas-voting" "polling-places" "voter-registration"
    "other"})

(defn valid-voter-service-type? [voter-service-type]
  (voter-service-types voter-service-type))

(defn validate-voter-service-type-format [{:keys [import-id] :as ctx}]
  (log/info "Validating VoterService/Type elements")
  (let [voter-service-type-simple-path (postgres/path->ltree
                                        "VipObject.ElectionAdministration.Department.VoterService.Type")
        imported-voter-service-types (korma/select postgres/xml-tree-values
                                       (korma/where {:results_id import-id
                                                     :simple_path voter-service-type-simple-path}))
        invalid-voter-service-types (remove
                                     (comp valid-voter-service-type? :value)
                                     imported-voter-service-types)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :election-administration
                          (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-voter-service-types)))
