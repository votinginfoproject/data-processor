(ns vip.data-processor.output.contest
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     ballot_id
                     ballot_placement
                     election_id
                     electoral_district_id
                     electorate_specifications
                     filing_closed_date
                     number_elected
                     number_voting_for
                     office
                     partisan
                     primary_party
                     special
                     type]}]
  (let [children [(xml-node ballot_id)
                  (xml-node ballot_placement)
                  (xml-node election_id)
                  (xml-node electoral_district_id)
                  (xml-node electorate_specifications)
                  (xml-node filing_closed_date)
                  (xml-node number_elected)
                  (xml-node number_voting_for)
                  (xml-node office)
                  (boolean-xml-node partisan)
                  (xml-node primary_party)
                  (boolean-xml-node special)
                  (xml-node type)]]
    (simple-xml :contest id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :contests])]
    (map ->xml (korma/select sql-table))))
