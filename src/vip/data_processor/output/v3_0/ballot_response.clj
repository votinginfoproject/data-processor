(ns vip.data-processor.output.v3-0.ballot-response
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     text
                     sort_order]}]
  (let [children [(xml-node text)
                  (xml-node sort_order)]]
    (simple-xml :ballot_response id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :ballot-responses])]
    (map ->xml (korma/select sql-table))))
