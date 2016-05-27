(ns vip.data-processor.db.translations.v5-1.candidate-contests-postgres-test
  (:require [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.candidate-contests
             :as candidate-contests]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.test-helpers :refer :all]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres contest->ltree-entries-test
  (testing "tests run the important function"
    (let [ctx {:input (csv-inputs ["5-1/candidate_contest.txt"])
               :spec-version "5.1"
               :ltree-index 0
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [candidate-contests/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
                           "cancon001" "VipObject.0.CandidateContest.0.id"
                           "Bad Guys" "VipObject.0.CandidateContest.0.BallotTitle.2.Text.0"
                           "en" "VipObject.0.CandidateContest.0.BallotTitle.2.Text.0.language"
                           "fips" "VipObject.0.CandidateContest.0.ExternalIdentifiers.5.ExternalIdentifier.0.Type.0"
                           "23" "VipObject.0.CandidateContest.0.ExternalIdentifiers.5.ExternalIdentifier.0.Value.1"))))
