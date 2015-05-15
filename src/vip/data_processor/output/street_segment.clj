(ns vip.data-processor.output.street-segment
  (:require [vip.data-processor.output.address :refer [address]]
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
        total (-> (korma/select sql-table
                                (korma/aggregate (count "*") :cnt))
                  first
                  :cnt)]
    (letfn [(chunked-sexps [page]
              (let [offset (* page chunk-size)]
                (when (< offset total)
                  (lazy-cat
                   (let [street-segments (korma/select sql-table
                                                       (korma/offset offset)
                                                       (korma/limit chunk-size))]
                     (map ->xml street-segments))
                   (chunked-sexps (inc page))))))]
      (chunked-sexps 0))))
