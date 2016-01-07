(ns vip.data-processor.output.v3-0.precinct
  (:require [korma.core :as korma]
            [vip.data-processor.output.xml-helpers :refer :all]))

(defn ->xml [ctx {:keys [id
                         name
                         number
                         locality_id
                         ward
                         mail_only
                         ballot_style_image_url]}]
  (let [polling-location-nodes (joined-nodes ctx :precinct id :polling-location)
        early-vote-site-nodes (joined-nodes ctx :precinct id :early-vote-site)
        electoral-district-nodes (joined-nodes ctx :precinct id :electoral-district)

        children (concat [(xml-node name)
                          (xml-node number)
                          (xml-node locality_id)]
                         electoral-district-nodes
                         [(xml-node ward)
                          (boolean-xml-node mail_only)]
                         polling-location-nodes
                         early-vote-site-nodes
                         [(xml-node ballot_style_image_url)])]
    (simple-xml :precinct id children)))

(defn xml-nodes [ctx]
  (let [precinct-table (get-in ctx [:tables :precincts])]
    (map (partial ->xml ctx) (korma/select precinct-table))))
