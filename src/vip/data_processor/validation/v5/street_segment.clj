(ns vip.data-processor.validation.v5.street-segment
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.string :as str]))

(util/validate-no-missing-values :street-segment
                                 [:odd-even-both]
                                 [:city]
                                 [:state]
                                 [:street-name]
                                 [:zip])

(def validate-odd-even-both-value
  (util/validate-enum-elements :oeb-enum :errors))

(def overlap-query
  "SELECT subpath(xtv.path,0,4) AS path, ss2.id AS ss2_id
   FROM v5_1_street_segments ss
   INNER JOIN v5_1_street_segments ss2
           ON ss2.start_house_number >= ss.start_house_number AND
              ss2.start_house_number <= ss.end_house_number AND
              ss.street_name = ss2.street_name AND
              ss.city = ss2.city AND
              ss.state = ss2.state AND
              ss.zip = ss2.zip AND
              COALESCE(ss.address_direction, 'NULL_VALUE') =
                COALESCE(ss2.address_direction, 'NULL_VALUE') AND
              COALESCE(ss.street_direction, 'NULL_VALUE') =
                COALESCE(ss2.street_direction, 'NULL_VALUE') AND
              COALESCE(ss.street_suffix, 'NULL_VALUE') =
                COALESCE(ss2.street_suffix, 'NULL_VALUE') AND
              ss.precinct_id != ss2.precinct_id AND
              ss.id != ss2.id AND
              ss.results_id = ss2.results_id AND
              (ss.odd_even_both = ss2.odd_even_both OR
               ss.odd_even_both = 'both' OR
               ss2.odd_even_both = 'both')
   INNER JOIN xml_tree_values xtv
           ON xtv.value = ss.id
   WHERE ss.results_id = ? AND xtv.simple_path = 'VipObject.StreetSegment.id';")

(defn validate-no-street-segment-overlaps
  [{:keys [import-id] :as ctx}]
  (let [overlaps (korma/exec-raw
                  (:conn postgres/v5-1-street-segments)
                  [overlap-query [import-id]]
                  :results)]
    (reduce (fn [ctx overlap]
              (update-in ctx [:errors
                              :street-segment
                              (.getValue (:path overlap))
                              :overlaps]
                         conj (:ss2_id overlap)))
            ctx overlaps)))
