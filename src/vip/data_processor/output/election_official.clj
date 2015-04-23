(ns vip.data-processor.output.election-official
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     name
                     title
                     phone
                     fax
                     email]}]
  (let [children [(xml-node name)
                  (xml-node title)
                  (xml-node phone)
                  (xml-node fax)
                  (xml-node email)]]
    (simple-xml :election_official id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :election-officials])]
    (map ->xml (korma/select sql-table))))
