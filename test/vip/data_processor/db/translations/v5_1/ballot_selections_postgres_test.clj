(ns vip.data-processor.db.translations.v5-1.ballot-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.ballot-selections :as bs]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres ballot-selection-transforms-test
  (testing "election.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/ballot_selection.txt"])
               :spec-version "5.1"
               :ltree-index 100
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [bs/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "bs001" "VipObject.0.BallotSelection.100.id"
        "1" "VipObject.0.BallotSelection.100.SequenceOrder.0"
        "bs002" "VipObject.0.BallotSelection.101.id"
        "2" "VipObject.0.BallotSelection.101.SequenceOrder.0"
        "bs003" "VipObject.0.BallotSelection.102.id"
        "3" "VipObject.0.BallotSelection.102.SequenceOrder.0"))))
