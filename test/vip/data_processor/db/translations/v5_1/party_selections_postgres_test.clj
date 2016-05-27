(ns vip.data-processor.db.translations.v5-1.party-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.translations.v5-1.party-selections :as ps]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres -transformer-test
  (testing "party_selection.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/party_selection.txt"])
               :spec-version "5.1"
               :ltree-index 100
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [ps/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "ps001" "VipObject.0.PartySelection.100.id"
        "1" "VipObject.0.PartySelection.100.SequenceOrder.0"
        "p001 p004" "VipObject.0.PartySelection.100.PartyIds.1"
        "ps002" "VipObject.0.PartySelection.101.id"
        "2" "VipObject.0.PartySelection.101.SequenceOrder.0"
        "p001 p002" "VipObject.0.PartySelection.101.PartyIds.1"
        "ps003" "VipObject.0.PartySelection.102.id"
        "3" "VipObject.0.PartySelection.102.SequenceOrder.0"
        "p003" "VipObject.0.PartySelection.102.PartyIds.1"))))
