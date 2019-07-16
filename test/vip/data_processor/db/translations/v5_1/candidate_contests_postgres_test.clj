(ns vip.data-processor.db.translations.v5-1.candidate-contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.candidate-contests :as cc]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres contest->ltree-entries-test
  (testing "tests run the important function"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/candidate_contest.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          cc/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values
       out-ctx
       "cancon001" "VipObject.0.CandidateContest.0.id"
       "Bad Guys" "VipObject.0.CandidateContest.0.BallotTitle.2.Text.0"
       "en" "VipObject.0.CandidateContest.0.BallotTitle.2.Text.0.language"
       "fips" "VipObject.0.CandidateContest.0.ExternalIdentifiers.5.ExternalIdentifier.0.Type.0"
       "23" "VipObject.0.CandidateContest.0.ExternalIdentifiers.5.ExternalIdentifier.0.Value.1"))))
