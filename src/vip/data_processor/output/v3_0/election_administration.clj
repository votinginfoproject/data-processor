(ns vip.data-processor.output.v3-0.election-administration
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [vip.data-processor.output.v3-0.address :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     name
                     eo_id
                     ovc_id
                     elections_url
                     registration_url
                     am_i_registered_url
                     absentee_url
                     where_do_i_vote_url
                     what_is_on_my_ballot_url
                     rules_url
                     voter_services
                     hours] :as election-administration}]
  (let [children [(xml-node name)
                  (xml-node eo_id)
                  (xml-node ovc_id)
                  (address :physical_address election-administration)
                  (address :mailing_address election-administration)
                  (xml-node elections_url)
                  (xml-node registration_url)
                  (xml-node am_i_registered_url)
                  (xml-node absentee_url)
                  (xml-node where_do_i_vote_url)
                  (xml-node what_is_on_my_ballot_url)
                  (xml-node rules_url)
                  (xml-node voter_services)
                  (xml-node hours)]]
    (simple-xml :election_administration id children)))

(defn xml-nodes [ctx]
  (let [election-administration-table (get-in ctx [:tables :election-administrations])]
    (map ->xml (korma/select election-administration-table))))
