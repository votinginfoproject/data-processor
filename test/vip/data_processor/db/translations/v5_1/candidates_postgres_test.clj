(ns vip.data-processor.db.translations.v5-1.candidates-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.candidates :as c]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres candidates-transforms-test
  (testing "candidate.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-1/candidate.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          c/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
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
