(ns vip.data-processor.validation.v5.polling-location
  (:require [vip.data-processor.errors :as errors]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

(def validate-no-missing-address-lines
  (util/validate-no-missing-elements :polling-location [:address-line]))

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
  (log/info "Checking for Polling Places mapped to multiple levels for import: " import-id)
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
      (errors/add-errors ctx :warning :id :global
                         :multiple-polling-locations-mappings
                         (str/join " " ids))
      ctx)))
