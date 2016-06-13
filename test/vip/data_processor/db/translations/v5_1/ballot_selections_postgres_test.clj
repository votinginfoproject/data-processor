(ns vip.data-processor.db.translations.v5-1.ballot-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres ballot-selection-transforms-test
  (testing "ballot_selection.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/ballot_selection.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "bs001" "VipObject.0.BallotSelection.0.id"
        "1" "VipObject.0.BallotSelection.0.SequenceOrder.0"
        "bs002" "VipObject.0.BallotSelection.1.id"
        "2" "VipObject.0.BallotSelection.1.SequenceOrder.0"
        "bs003" "VipObject.0.BallotSelection.2.id"
        "3" "VipObject.0.BallotSelection.2.SequenceOrder.0"))))
