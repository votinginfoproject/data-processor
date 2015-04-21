(ns vip.data-processor.output.ballot
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn candidate->xml [candidate]
  (let [xml {:tag :candidate_id
             :content [(str (:id candidate))]}
        sort-order (:sort_order candidate)]
    (if sort-order
      (assoc-in xml [:attrs :sort_order] sort-order)
      xml)))

(defn ->xml [ballot candidates]
  (let [{:keys [id
                referendum_id
                custom_ballot_id
                write_in
                image_url]} ballot
        children (concat [(xml-node referendum_id)
                          (xml-node custom_ballot_id)
                          (boolean-xml-node write_in)
                          (xml-node image_url)]
                         (map candidate->xml candidates))]
    (simple-xml :ballot id children)))

(defn candidates [ctx ballot]
  (let [ballot-candidates-table (get-in ctx [:tables :ballot-candidates])
        candidates-table (get-in ctx [:tables :candidates])]
    (korma/select ballot-candidates-table
                  (korma/fields :candidates.id :candidates.sort_order)
                  (korma/join candidates-table
                              (= :candidates.id :candidate_id))
                  (korma/where {:ballot_id (:id ballot)}))))

(defn xml-nodes [ctx]
  (let [ballots-table (get-in ctx [:tables :ballots])
        ballots (korma/select ballots-table)]
    (map #(->xml % (candidates ctx %))
         ballots)))
