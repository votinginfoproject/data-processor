(ns vip.data-processor.db.translations.v5-1.election-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.translations.v5-1.election :as election]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres election-transforms-test
  (testing "election.txt is loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/election.txt"])
               :spec-version "5.1"
               :ltree-index 1
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [election/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
        "e001" "VipObject.0.Election.1.id"
        "Best Hot Dog" "VipObject.0.Election.1.Name.6.Text.0"
        "en" "VipObject.0.Election.1.Name.6.Text.0.language"
        "Edible" "VipObject.0.Election.1.ElectionType.2.Text.0"
        "en" "VipObject.0.Election.1.ElectionType.2.Text.0.language"
        "s050" "VipObject.0.Election.1.StateId.11"
        "10/08/2016" "VipObject.0.Election.1.Date.1"))))
