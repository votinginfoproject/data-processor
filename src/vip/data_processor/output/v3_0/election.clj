(ns vip.data-processor.output.v3-0.election
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     absentee_ballot_info
                     absentee_request_deadline
                     date
                     election_day_registration
                     election_type
                     polling_hours
                     registration_deadline
                     registration_info
                     results_url
                     state_id
                     statewide]}]
  (let [children [(xml-node absentee_ballot_info)
                  (xml-node absentee_request_deadline)
                  (xml-node date)
                  (boolean-xml-node election_day_registration)
                  (xml-node election_type)
                  (xml-node polling_hours)
                  (xml-node registration_deadline)
                  (xml-node registration_info)
                  (xml-node results_url)
                  (xml-node state_id)
                  (boolean-xml-node statewide)]]
    (simple-xml :election id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :elections])]
    (map ->xml (korma/select sql-table))))
