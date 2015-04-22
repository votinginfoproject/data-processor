(ns vip.data-processor.output.precinct
  (:require [korma.core :as korma]
            [vip.data-processor.output.xml-helpers :refer :all]))

(defn precinct-polling-location->node [{:keys [polling_location_id]}]
  (xml-node polling_location_id))

(defn precinct-early-vote-site->node [{:keys [early_vote_site_id]}]
  (xml-node early_vote_site_id))

(defn precinct-electoral-district->node [{:keys [electoral_district_id]}]
  (xml-node electoral_district_id))

(defn ->xml [ctx {:keys [id
                         name
                         number
                         locality_id
                         ward
                         mail_only
                         ballot_style_image_url]}]
  (let [precinct-polling-location-table (get-in ctx [:tables :precinct-polling-locations])
        polling-location-ids (korma/select precinct-polling-location-table
                                           (korma/where {:precinct_id id}))
        polling-location-nodes (map precinct-polling-location->node polling-location-ids)

        precinct-early-vote-site-table (get-in ctx [:tables :precinct-early-vote-sites])
        early-vote-site-ids (korma/select precinct-early-vote-site-table
                                           (korma/where {:precinct_id id}))
        early-vote-site-nodes (map precinct-early-vote-site->node early-vote-site-ids)

        precinct-electoral-district-table (get-in ctx [:tables :precinct-electoral-districts])
        electoral-district-ids (korma/select precinct-electoral-district-table
                                           (korma/where {:precinct_id id}))
        electoral-district-nodes (map precinct-electoral-district->node electoral-district-ids)

        children (concat [(xml-node name)
                          (xml-node number)
                          (xml-node locality_id)
                          (xml-node ward)
                          (boolean-xml-node mail_only)
                          (xml-node ballot_style_image_url)]
                         polling-location-nodes
                         early-vote-site-nodes
                         electoral-district-nodes)]
    (simple-xml :precinct id children)))

(defn xml-nodes [ctx]
  (let [precinct-table (get-in ctx [:tables :precincts])]
    (map (partial ->xml ctx) (korma/select precinct-table))))
