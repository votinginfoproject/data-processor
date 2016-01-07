(ns vip.data-processor.output.v3-0.ballot-line-result
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     contest_id
                     jurisdiction_id
                     entire_district
                     candidate_id
                     ballot_response_id
                     votes
                     victorious
                     certification]}]
  (let [children [(xml-node contest_id)
                  (xml-node jurisdiction_id)
                  (boolean-xml-node entire_district)
                  (xml-node candidate_id)
                  (xml-node ballot_response_id)
                  (xml-node votes)
                  (boolean-xml-node victorious)]]
    (assoc-in (simple-xml :ballot_line_result id children)
              [:attrs :certification] certification)))


(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :ballot-line-results])]
    (map ->xml (korma/select sql-table))))
