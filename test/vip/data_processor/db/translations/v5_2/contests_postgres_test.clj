(ns vip.data-processor.db.translations.v5-2.contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-2.contests :as c]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-2 :as v5-2]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres contest->ltree-entries-test
  (testing "tests run the important function"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-2/contest.txt"])
               :errors-chan errors-chan
               :spec-version "5.2"
               :spec-family "5.2"
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-2/data-specs)
                          postgres/prep-v5-2-run
                          process/process-v5-validations
                          csv/load-csvs
                          c/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values out-ctx
       "con001" "VipObject.0.Contest.0.id"
       "Die Hard Series" "VipObject.0.Contest.0.BallotTitle.3.Text.0"
       "en" "VipObject.0.Contest.0.BallotTitle.3.Text.0.language"
       "fips" "VipObject.0.Contest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Type.0"
       "23" "VipObject.0.Contest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Value.1"))))
