(ns vip.data-processor.validation.v5.polling-location
  (:require [vip.data-processor.errors :as errors]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

(def validate-no-missing-address
  (util/build-xml-tree-value-query-validator
   :errors :polling-location :missing :missing-either-address-line-or-address-structured
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) AS path,
          parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,2) = 'VipObject.PollingLocation') xtv
    LEFT JOIN (SELECT parent_with_id, path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.PollingLocation.AddressLine') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id
    LEFT JOIN (SELECT parent_with_id, path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.PollingLocation.AddressStructured') xtv3
    ON xtv.parent_with_id = xtv3.parent_with_id
    WHERE xtv2.path IS NULL AND xtv3.path IS NULL"
   (fn [{:keys [import-id]}]
     [import-id import-id import-id])))

(def validate-no-missing-latitudes
  (util/validate-no-missing-elements :lat-lng [:latitude]))

(def validate-no-missing-longitudes
  (util/validate-no-missing-elements :lat-lng [:longitude]))

(def validate-latitude
  (util/validate-elements :lat-lng
                          [:latitude]
                          (comp float? edn/read-string)
                          :errors :format))

(def validate-longitude
  (util/validate-elements :lat-lng
                          [:longitude]
                          (comp float? edn/read-string)
                          :errors :format))

(defn check-for-polling-locations-mapped-to-multiple-places
  "Sometimes Polling Locations are mistakenly scoped in
   both Localities and Precincts. Usually, though not always,
   this is a misunderstanding, so let's warn if it happens."
  [{:keys [import-id] :as ctx}]
  (log/info "Checking for Polling Places mapped to localities and precincts")
  (let [results
        (korma/exec-raw
         (:conn postgres/xml-tree-values)
         ["with locality_polling_locations AS
             (select unnest(regexp_split_to_array(value, E'\\\\s+')) as id
              from xml_tree_values where results_id = ? AND
              path ~ 'VipObject.*.Locality.*.PollingLocationIds.*'),
           precinct_polling_locations AS
             (select unnest(regexp_split_to_array(value, E'\\\\s+')) as id from
              xml_tree_values where results_id = ? AND
              path ~ 'VipObject.*.Precinct.*.PollingLocationIds.*')
           select distinct lpl.id as dup_id from locality_polling_locations lpl
             join precinct_polling_locations ppl on lpl.id = ppl.id"
          [import-id import-id]]
         :results)
        ids (map :dup_id results)]
    (if (seq ids)
      (errors/add-errors ctx :warnings :id :global
                         :multiple-polling-locations-mappings
                         (str/join " " ids))
      ctx)))

(def validate-structured-address-line-1
  (util/build-xml-tree-value-query-validator
   :errors :polling-location :missing :missing-address-structured-line-1
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 6) || 'Line1' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 3) = 'VipObject.PollingLocation.AddressStructured') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 7)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-structured-address-city
  (util/build-xml-tree-value-query-validator
   :errors :polling-location :missing :missing-address-structured-city
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 6) || 'City' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 3) = 'VipObject.PollingLocation.AddressStructured') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 7)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-structured-address-state
  (util/build-xml-tree-value-query-validator
   :errors :polling-location :missing :missing-address-structured-state
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 6) || 'State' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path, 0, 3) = 'VipObject.PollingLocation.AddressStructured') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 7)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))
