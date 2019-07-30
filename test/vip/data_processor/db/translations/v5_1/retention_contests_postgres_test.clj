(ns vip.data-processor.db.translations.v5-1.retention-contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.retention-contests :as rc]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "retention_contest.txt is loaded and transformed"
    (let [csv-files (csv-inputs ["5-1/retention_contest.txt"])
errors-chan (a/chan 100)
          ctx {:csv-source-file-paths csv-files
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-1/data-specs)
                          postgres/prep-v5-1-run
                          process/process-v5-validations
                          csv/load-csvs
                          rc/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values out-ctx
        "rc001" "VipObject.0.RetentionContest.0.id"
        "Judge Dredd" "VipObject.0.RetentionContest.0.Abbreviation.0"
        "bs001" "VipObject.0.RetentionContest.0.BallotSelectionIds.1"
        "Let Judge Dredd keep his job?" "VipObject.0.RetentionContest.0.BallotTitle.2.Text.0"
        "en" "VipObject.0.RetentionContest.0.BallotTitle.2.Text.0.language"
        "ed001" "VipObject.0.RetentionContest.0.ElectoralDistrictId.3"
        "dredd" "VipObject.0.RetentionContest.0.Name.4"
        "1" "VipObject.0.RetentionContest.0.SequenceOrder.5"
        "can001" "VipObject.0.RetentionContest.0.CandidateId.6"
        "off001" "VipObject.0.RetentionContest.0.OfficeId.7"))))
