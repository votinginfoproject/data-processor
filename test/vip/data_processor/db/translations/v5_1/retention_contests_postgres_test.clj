(ns vip.data-processor.db.translations.v5-1.retention-contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.retention-contests :as rc]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "retention_contest.txt is loaded and transformed"
    (let [csv-files (csv-inputs ["5-1/retention_contest.txt"])
          ctx {:input csv-files
               :spec-version "5.1"
               :ltree-index 10
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [rc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "rc001" "VipObject.0.RetentionContest.10.id"
        "Judge Dredd" "VipObject.0.RetentionContest.10.Abbreviation.0"
        "bs001" "VipObject.0.RetentionContest.10.BallotSelectionIds.1"
        "Let Judge Dredd keep his job?" "VipObject.0.RetentionContest.10.BallotTitle.2.Text.0"
        "en" "VipObject.0.RetentionContest.10.BallotTitle.2.Text.0.language"
        "ed001" "VipObject.0.RetentionContest.10.ElectoralDistrictId.3"
        "dredd" "VipObject.0.RetentionContest.10.Name.4"
        "1" "VipObject.0.RetentionContest.10.SequenceOrder.5"
        "can001" "VipObject.0.RetentionContest.10.CandidateId.6"
        "off001" "VipObject.0.RetentionContest.10.OfficeId.7"))))
