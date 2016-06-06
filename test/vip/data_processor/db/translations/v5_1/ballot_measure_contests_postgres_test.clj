(ns vip.data-processor.db.translations.v5-1.ballot-measure-contests-postgres-test
  (:require [clojure.test :refer [deftest testing is are use-fixtures]]
            [korma.core :as korma]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.ballot-measure-contests :as bmc]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "ballot_measure_contest.txt is loaded and transformed"
    (let [csv-files (csv-inputs ["5-1/ballot_measure_contest.txt"])
          ctx {:input csv-files
               :spec-version "5.1"
               :ltree-index 0
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [bmc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "part duex" "VipObject.0.BallotMeasureContest.0.BallotSubTitle.2.Text.0"
        "hot shots" "VipObject.0.BallotMeasureContest.0.BallotTitle.3.Text.0"
        "en" "VipObject.0.BallotMeasureContest.0.BallotTitle.3.Text.0.language"
        "bs001 bs002 bs003" "VipObject.0.BallotMeasureContest.0.BallotSelectionIds.1"
        "fips" "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Type.0"
        "54" "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Value.1")

      (is (= 30
             (count (korma/select postgres/xml-tree-values
                      (korma/where {:results_id (:import-id out-ctx)}))))))))
