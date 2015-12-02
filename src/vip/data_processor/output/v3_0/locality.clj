(ns vip.data-processor.output.v3-0.locality
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [ctx {:keys [id
                         name
                         state_id
                         type
                         election_administration_id] :as locality}]
  (let [children (concat [(xml-node name)
                          (xml-node state_id)
                          (xml-node type)
                          (xml-node election_administration_id)]
                         (joined-nodes ctx :locality id :early-vote-site))]
    (simple-xml :locality id children)))

(defn xml-nodes [ctx]
  (let [localities-table (get-in ctx [:tables :localities])]
    (map (partial ->xml ctx) (korma/select localities-table))))
