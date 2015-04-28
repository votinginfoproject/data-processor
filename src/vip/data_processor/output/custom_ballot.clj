(ns vip.data-processor.output.custom-ballot
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ballot-responses
  "Find ballot responses belonging to a custom ballot. Goes through
  the custom_ballot_ballot_responses table to grab their `id` and
  `sort_order` columns, which is all that's needed for the custom
  ballot's XML representation."
  [ctx custom-ballot]
  (let [custom-ballot-ballot-responses-table (get-in ctx [:tables :custom-ballot-ballot-responses])
        ballot-responses-table (get-in ctx [:tables :ballot-responses])]
    (korma/select custom-ballot-ballot-responses-table
                  (korma/fields :ballot_responses.id :ballot_responses.sort_order)
                  (korma/join ballot-responses-table
                              (= :ballot_responses.id :ballot_response_id))
                  (korma/where {:custom_ballot_id (:id custom-ballot)}))))

(defn ballot-response->xml [ballot-response]
  (let [xml {:tag :ballot_response_id
             :content [(str (:id ballot-response))]}]
    (if-let [sort-order (:sort_order ballot-response)]
      (assoc-in xml [:attrs :sort_order] sort-order)
      xml)))

(defn ->xml [custom-ballot ballot-responses]
  (let [{:keys [id heading]} custom-ballot
        children (concat [(xml-node heading)]
                         (map ballot-response->xml ballot-responses))]
    (simple-xml :custom_ballot id children)))

(defn xml-nodes [ctx]
  (let [custom-ballots-table (get-in ctx [:tables :custom-ballots])
        custom-ballots (korma/select custom-ballots-table)]
    (map #(->xml % (ballot-responses ctx %))
         custom-ballots)))
