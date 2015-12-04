(ns vip.data-processor.output.v3-0.electoral-district
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     name
                     type
                     number]}]
  (let [children [(xml-node name)
                  (xml-node type)
                  (xml-node number)]]
    (simple-xml :electoral_district id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :electoral-districts])]
    (map ->xml (korma/select sql-table))))
