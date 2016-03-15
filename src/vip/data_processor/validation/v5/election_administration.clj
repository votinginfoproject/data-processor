(ns vip.data-processor.validation.v5.election-administration
  (:require [vip.data-processor.validation.v5.util :as util]
            [clojure.string :as str]))

(def validate-no-missing-departments
  (util/build-xml-tree-value-query-validator
   :errors :election-administration :missing :missing-department
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Department' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 4) ~ 'VipObject.0.ElectionAdministration.*{1}') xtv
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
  (let [voter-service-type-path (str/join "."
                                          ["VipObject" "0"
                                           "ElectionAdministration" "*{1}"
                                           "Department" "*{1}"
                                           "VoterService" "*{1}"
                                           "Type" "*{1}"])
        imported-voter-service-types (util/select-lquery
                                      import-id
                                      voter-service-type-path)
        invalid-voter-service-types (remove
                                     (comp valid-voter-service-type? :value)
                                     imported-voter-service-types)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :election-administration
                          (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-voter-service-types)))
