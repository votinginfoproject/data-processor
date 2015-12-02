(ns vip.data-processor.output.v3-0.polling-location
  (:require [vip.data-processor.output.v3-0.address :refer [address]]
            [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     directions
                     polling_hours
                     photo_url] :as polling-location}]
  (let [children [(address :address polling-location)
                  (xml-node directions)
                  (xml-node polling_hours)
                  (xml-node photo_url)]]
    (simple-xml :polling_location id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :polling-locations])]
    (map ->xml (korma/select sql-table))))
