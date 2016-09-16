(ns vip.data-processor.db.translations.v5-1.party-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres -transformer-test
  (testing "party_selection.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/party_selection.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems-2 errors {})
      (are-xml-tree-values out-ctx
        "ps001" "VipObject.0.PartySelection.0.id"
        "1" "VipObject.0.PartySelection.0.SequenceOrder.0"
        "p001 p004" "VipObject.0.PartySelection.0.PartyIds.1"
        "ps002" "VipObject.0.PartySelection.1.id"
        "2" "VipObject.0.PartySelection.1.SequenceOrder.0"
        "p001 p002" "VipObject.0.PartySelection.1.PartyIds.1"
        "ps003" "VipObject.0.PartySelection.2.id"
        "3" "VipObject.0.PartySelection.2.SequenceOrder.0"
        "p003" "VipObject.0.PartySelection.2.PartyIds.1"))))
