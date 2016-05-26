(ns vip.data-processor.db.translations.v5-1.ordered-contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.ordered-contests :as oc]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "ordered_contest.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/ordered_contest.txt"])
               :spec-version "5.1"
               :ltree-index 1
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [oc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
                           "oc2025" "VipObject.0.OrderedContest.1.id"
                           "bs01 bs05 bs02" "VipObject.0.OrderedContest.1.OrderedBallotSelectionIds.1"
                           "con02" "VipObject.0.OrderedContest.2.ContestId.0"))))
