(ns vip.data-processor.output.street-segment
  (:require [vip.data-processor.db.util :as util]
            [vip.data-processor.output.address :refer [address]]
            [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     start_house_number
                     end_house_number
                     odd_even_both
                     start_apartment_number
                     end_apartment_number
                     precinct_id
                     precinct_split_id] :as street-segment}]
  (let [children [(address :non_house_address street-segment)
                  (xml-node start_house_number)
                  (xml-node end_house_number)
                  (xml-node odd_even_both)
                  (xml-node start_apartment_number)
                  (xml-node end_apartment_number)
                  (xml-node precinct_id)
                  (xml-node precinct_split_id)]]
    (simple-xml :street_segment id children)))

(def chunk-size 10000)

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :street-segments])
        street-segments (util/select-*-lazily chunk-size sql-table)]
    (map ->xml street-segments)))
