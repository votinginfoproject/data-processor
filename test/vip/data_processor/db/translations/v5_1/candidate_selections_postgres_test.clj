(ns vip.data-processor.db.translations.v5-1.candidate-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.candidate-selections :as cs]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidate-selection-transforms-test
  (testing "candidate_selection.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/candidate_selection.txt"])
               :spec-version "5.1"
               :ltree-index 14
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [cs/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "cs001" "VipObject.0.CandidateSelection.14.id"
        "3" "VipObject.0.CandidateSelection.14.SequenceOrder.0"
        "par04 par05" "VipObject.0.CandidateSelection.15.EndorsementPartyIds.2"
        "cs003" "VipObject.0.CandidateSelection.16.id"
        "1" "VipObject.0.CandidateSelection.16.SequenceOrder.0"
        "can124" "VipObject.0.CandidateSelection.16.CandidateIds.1"
        "true" "VipObject.0.CandidateSelection.16.IsWriteIn.3"))))
