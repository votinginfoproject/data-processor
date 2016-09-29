(ns vip.data-processor.db.translations.v5-1.election-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres election-transforms-test
  (testing "election.txt is loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/election.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values out-ctx
        "e001" "VipObject.0.Election.0.id"
        "Best Hot Dog" "VipObject.0.Election.0.Name.6.Text.0"
        "en" "VipObject.0.Election.0.Name.6.Text.0.language"
        "Edible" "VipObject.0.Election.0.ElectionType.2.Text.0"
        "en" "VipObject.0.Election.0.ElectionType.2.Text.0.language"
        "s050" "VipObject.0.Election.0.StateId.11"
        "10/08/2016" "VipObject.0.Election.0.Date.1"))))
