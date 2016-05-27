(ns vip.data-processor.db.sqlite-test
  (:require [vip.data-processor.db.sqlite :as sqlite]
            [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]))

(deftest column-names-test
  (let [db (sqlite/temp-db "column-names-test" "3.0")]
    (is (= ["state_id" "early_vote_site_id"]
           (sqlite/column-names (-> db :tables :state-early-vote-sites))))))
