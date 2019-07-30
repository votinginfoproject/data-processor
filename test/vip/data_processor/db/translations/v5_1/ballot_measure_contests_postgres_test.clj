(ns vip.data-processor.db.translations.v5-1.ballot-measure-contests-postgres-test
  (:require [clojure.test :refer [deftest testing is are use-fixtures]]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.ballot-measure-contests :as bmc]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "ballot_measure_contest.txt is loaded and transformed"
    (let [csv-file-paths (csv-inputs ["5-1/ballot_measure_contest.txt"])
          errors-chan (a/chan 100)
          ctx {:csv-source-file-paths csv-file-paths
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :spec-family "5.1"
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          bmc/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
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
