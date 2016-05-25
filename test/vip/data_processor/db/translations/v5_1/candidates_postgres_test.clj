(ns vip.data-processor.db.translations.v5-1.candidates-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.candidates :as c]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidates-transforms-test
  (testing "candidate.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/candidate.txt"])
               :spec-version "5.1"
               :ltree-index 40
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [c/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "can001" "VipObject.0.Candidate.40.id"
        "Jude Fawley" "VipObject.0.Candidate.40.BallotName.0.Text.0"
        "en" "VipObject.0.Candidate.40.BallotName.0.Text.0.language"
        "local-level" "VipObject.0.Candidate.40.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
        "stonemason-7" "VipObject.0.Candidate.40.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"
        "filed" "VipObject.0.Candidate.40.PreElectionStatus.7"

        "can002" "VipObject.0.Candidate.41.id"
        "Arabella Donn" "VipObject.0.Candidate.41.BallotName.0.Text.0"
        "projected-winner" "VipObject.0.Candidate.41.PostElectionStatus.7"
        "qualified" "VipObject.0.Candidate.41.PreElectionStatus.8"))))
