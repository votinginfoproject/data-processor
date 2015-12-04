(ns vip.data-processor.output.v3-0.early-vote-site
  (:require [vip.data-processor.output.v3-0.address :refer [address]]
            [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     name
                     directions
                     voter_services
                     start_date
                     end_date
                     days_times_open] :as early-vote-site}]
  (let [children [(address :address early-vote-site)
                  (xml-node name)
                  (xml-node directions)
                  (xml-node voter_services)
                  (xml-node start_date)
                  (xml-node end_date)
                  (xml-node days_times_open)]]
    (simple-xml :early_vote_site id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :early-vote-sites])]
    (map ->xml (korma/select sql-table))))
