(ns vip.data-processor.output.candidate
  (:require [vip.data-processor.output.address :refer [address]]
            [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                      name
                      party
                      candidate_url
                      biography
                      phone
                      photo_url
                      email
                      sort_order] :as candidate}]
  (let [children [(address :filed_mailing_address candidate)
                  (xml-node name)
                  (xml-node party)
                  (xml-node candidate_url)
                  (xml-node biography)
                  (xml-node phone)
                  (xml-node photo_url)
                  (xml-node email)
                  (xml-node sort_order)]]
    (simple-xml :candidate id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :candidates])]
    (map ->xml (korma/select sql-table))))
