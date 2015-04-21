(ns vip.data-processor.output.referendum
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ballot-response->xml [ballot-response]
  (let [xml {:tag :ballot_response_id
             :content [(str (:id ballot-response))]}
        sort-order (:sort_order ballot-response)]
    (if sort-order
      (assoc-in xml [:attrs :sort_order] sort-order)
      xml)))

(defn ->xml [referendum ballot-responses]
  (let [{:keys [id
                title
                subtitle
                brief
                text
                pro_statement
                con_statement
                passage_threshold
                effect_of_abstain]} referendum
        children (concat [(xml-node title)
                          (xml-node subtitle)
                          (xml-node brief)
                          (xml-node text)
                          (xml-node pro_statement)
                          (xml-node con_statement)
                          (xml-node passage_threshold)
                          (xml-node effect_of_abstain)]
                         (map ballot-response->xml ballot-responses))]
    (simple-xml :referendum id children)))

(defn xml-nodes [ctx]
  (let [referendums-table (get-in ctx [:tables :referendums])
        referendum-ballot-responses-table (get-in ctx [:tables :referendum-ballot-responses])
        ballot-responses-table (get-in ctx [:tables :ballot-responses])
        referendums (korma/select referendums-table)]
    (map #(->xml % (korma/select referendum-ballot-responses-table
                                                    (korma/fields :ballot_responses.id :ballot_responses.sort_order)
                                                    (korma/join ballot-responses-table
                                                                (= :ballot_responses.id :ballot_response_id))
                                                    (korma/where {:referendum_id (:id %)}))) referendums)))
