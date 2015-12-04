(ns vip.data-processor.output.v3-0.state
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [korma.core :as korma]))

(defn ->xml [{:keys [id
                     name
                     election_administration_id]}
             early-vote-site-ids]
  (let [early-vote-site-nodes (map #(assoc {:tag :early_vote_site} :content [%])
                                   early-vote-site-ids)
        children (concat [(xml-node name)
                          (xml-node election_administration_id)]
                         early-vote-site-nodes)]
    (simple-xml :state id children)))

(defn xml-nodes [ctx]
  (let [sql-table (get-in ctx [:tables :states])
        state-early-vote-sites-table (get-in ctx [:tables :state-early-vote-sites])
        states (korma/select sql-table)
        state-id (-> states first :id)
        early-vote-site-ids (as-> state-early-vote-sites-table
                                early-vote-sites
                              (korma/select early-vote-sites
                                            (korma/where {:state_id state-id}))
                              (map :early_vote_site_id early-vote-sites))]
    (map #(->xml % early-vote-site-ids) states)))
