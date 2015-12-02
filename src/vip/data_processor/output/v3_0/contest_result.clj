(ns vip.data-processor.output.v3-0.contest-result
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml-node [{:keys [id
                          contest_id
                          jurisdiction_id
                          entire_district
                          total_votes
                          total_valid_votes
                          overvotes
                          blank_votes
                          accepted_provisional_votes
                          rejected_votes
                          certification]}]
  (let [children [(xml-node contest_id)
                  (xml-node jurisdiction_id)
                  (boolean-xml-node entire_district)
                  (xml-node total_votes)
                  (xml-node total_valid_votes)
                  (xml-node overvotes)
                  (xml-node blank_votes)
                  (xml-node accepted_provisional_votes)
                  (xml-node rejected_votes)]]
    (assoc-in (simple-xml :contest_result id children)
              [:attrs :certification] certification)))

(defn xml-nodes [ctx]
  (let [contest-results-table (get-in ctx [:tables :contest-results])]
    (map ->xml-node (korma/select contest-results-table))))
