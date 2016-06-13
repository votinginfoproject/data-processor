(ns vip.data-processor.db.translations.v5-1.candidates-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidates-transforms-test
  (testing "candidate.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/candidate.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "can001" "VipObject.0.Candidate.0.id"
        "Jude Fawley" "VipObject.0.Candidate.0.BallotName.0.Text.0"
        "en" "VipObject.0.Candidate.0.BallotName.0.Text.0.language"
        "local-level" "VipObject.0.Candidate.0.ExternalIdentifiers.1.ExternalIdentifier.0.Type.0"
        "stonemason-7" "VipObject.0.Candidate.0.ExternalIdentifiers.1.ExternalIdentifier.0.Value.1"
        "filed" "VipObject.0.Candidate.0.PreElectionStatus.7"

        "can002" "VipObject.0.Candidate.1.id"
        "Arabella Donn" "VipObject.0.Candidate.1.BallotName.0.Text.0"
        "projected-winner" "VipObject.0.Candidate.1.PostElectionStatus.7"
        "qualified" "VipObject.0.Candidate.1.PreElectionStatus.8"))))
