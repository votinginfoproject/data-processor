(ns vip.data-processor.db.translations.v5-1.ballot-measure-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres ballot-measure-selection-transforms-test
  (testing "ballot_measure_selection.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/ballot_measure_selection.txt"])
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
        "bms001" "VipObject.0.BallotMeasureSelection.0.id"
        "1" "VipObject.0.BallotMeasureSelection.0.SequenceOrder.0"
        "Proposition A" "VipObject.0.BallotMeasureSelection.0.Selection.1.Text.0"
        "en" "VipObject.0.BallotMeasureSelection.0.Selection.1.Text.0.language"
        "bms002" "VipObject.0.BallotMeasureSelection.1.id"
        "2" "VipObject.0.BallotMeasureSelection.1.SequenceOrder.0"
        "Proposition B" "VipObject.0.BallotMeasureSelection.1.Selection.1.Text.0"
        "en" "VipObject.0.BallotMeasureSelection.1.Selection.1.Text.0.language"))))
