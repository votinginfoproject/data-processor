(ns vip.data-processor.db.translations.v5-1.ballot-measure-selections-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.translations.v5-1.ballot-measure-selections :as bms]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres ballot-measure-selection-transforms-test
  (testing "ballot_measure_selection.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/ballot_measure_selection.txt"])
               :spec-version "5.1"
               :ltree-index 2
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [bms/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "bms001" "VipObject.0.BallotMeasureSelection.2.id"
        "1" "VipObject.0.BallotMeasureSelection.2.SequenceOrder.0"
        "Proposition A" "VipObject.0.BallotMeasureSelection.2.Selection.1.Text.0"
        "en" "VipObject.0.BallotMeasureSelection.2.Selection.1.Text.0.language"
        "bms002" "VipObject.0.BallotMeasureSelection.3.id"
        "2" "VipObject.0.BallotMeasureSelection.3.SequenceOrder.0"
        "Proposition B" "VipObject.0.BallotMeasureSelection.3.Selection.1.Text.0"
        "en" "VipObject.0.BallotMeasureSelection.3.Selection.1.Text.0.language"))))
