(ns vip.data-processor.db.translations.v5-1.contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres contest->ltree-entries-test
  (testing "tests run the important function"
    (let [ctx {:input (csv-inputs ["5-1/contest.txt"])
               :spec-version "5.1"
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
       "con001" "VipObject.0.Contest.0.id"
       "Die Hard Series" "VipObject.0.Contest.0.BallotTitle.3.Text.0"
       "en" "VipObject.0.Contest.0.BallotTitle.3.Text.0.language"
       "fips" "VipObject.0.Contest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Type.0"
       "23" "VipObject.0.Contest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Value.1"))))
