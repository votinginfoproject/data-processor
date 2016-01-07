(ns vip.data-processor.output.v3-0.source
  (:require [korma.core :as korma]
            [vip.data-processor.output.xml-helpers :refer :all]))

(defn ->xml [{:keys [id
                     name
                     vip_id
                     datetime
                     description
                     organization_url
                     feed_contact_id
                     tou_url]}]
  (simple-xml :source id [(xml-node name)
                          (xml-node vip_id)
                          (xml-node datetime)
                          (xml-node description)
                          (xml-node organization_url)
                          (xml-node feed_contact_id)
                          (xml-node tou_url)]))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :sources])]
    (map ->xml (korma/select sql-table))))
