(ns vip.data-processor.output.v3-0.precinct-split
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [ctx {:keys [id
                         name
                         precinct_id
                         ballot_style_image_url]}]
  (let [electoral-district-nodes (joined-nodes ctx :precinct-split id :electoral-district)
        polling-location-nodes (joined-nodes ctx :precinct-split id :polling-location)

        children (concat [(xml-node name)
                          (xml-node precinct_id)]
                         electoral-district-nodes
                         polling-location-nodes
                         [(xml-node ballot_style_image_url)])]
    (simple-xml :precinct_split id children)))

(defn xml-nodes [ctx]
  (let [precinct-splits-table (get-in ctx [:tables :precinct-splits])]
    (map (partial ->xml ctx) (korma/select precinct-splits-table))))
