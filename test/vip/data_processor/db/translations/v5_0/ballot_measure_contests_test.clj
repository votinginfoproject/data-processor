(ns vip.data-processor.db.translations.v5-0.ballot-measure-contests-test
  (:require [clojure.test :refer [deftest testing is are use-fixtures]]
            [korma.core :as korma]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-0.ballot-measure-contests :as bmc]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]))

(use-fixtures :once setup-postgres)

(deftest bmc->ltree-entries-test
  (testing "index numbers follow the order from the spec, skipping missing attributes"
    (let [bmc {:name "NAME"
               :full_text "FULL TEXT"
               :id "bmc002"
               :external_identifier_type "TYPE"
               :external_identifier_value "VALUE"}
          idx-fn (util/index-generator 1)
          ltree-paths (bmc/bmc->ltree-entries idx-fn bmc)]
      (is (= (set ltree-paths)
             #{{:path "VipObject.0.BallotMeasureContest.1.id"
                :simple_path "VipObject.BallotMeasureContest.id"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "bmc002"}
               {:path "VipObject.0.BallotMeasureContest.1.ExternalIdentifiers.0.ExternalIdentifier.0.Type.0"
                :simple_path "VipObject.BallotMeasureContest.ExternalIdentifiers.ExternalIdentifier.Type"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "TYPE"}
               {:path "VipObject.0.BallotMeasureContest.1.ExternalIdentifiers.0.ExternalIdentifier.0.Value.1"
                :simple_path "VipObject.BallotMeasureContest.ExternalIdentifiers.ExternalIdentifier.Value"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "VALUE"}
               {:path "VipObject.0.BallotMeasureContest.1.Name.1"
                :simple_path "VipObject.BallotMeasureContest.Name"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "NAME"}
               {:path "VipObject.0.BallotMeasureContest.1.FullText.2.Text.0"
                :simple_path "VipObject.BallotMeasureContest.FullText.Text"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "FULL TEXT"}
               {:path "VipObject.0.BallotMeasureContest.1.FullText.2.Text.0.language"
                :simple_path "VipObject.BallotMeasureContest.FullText.Text.language"
                :parent_with_id "VipObject.0.BallotMeasureContest.1.id"
                :value "en"}})))))

(deftest ^:postgres an-integration-test
  (testing "we can load a CSV and transform it"
    (let [csv-files (csv-inputs ["5-0/ballot_measure_contest.txt"])
          ctx {:input csv-files
               :spec-version "5.0"
               :ltree-index 0
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.0")
                          [bmc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are [value path] (= value
                           (->
                            (korma/select postgres/xml-tree-values
                              (korma/fields :value)
                              (korma/where {:results_id (:import-id out-ctx)
                                            :path (postgres/path->ltree path)}))
                            first
                            :value))
        "part duex" "VipObject.0.BallotMeasureContest.0.BallotSubTitle.2.Text.0"
        "hot shots" "VipObject.0.BallotMeasureContest.0.BallotTitle.3.Text.0"
        "en" "VipObject.0.BallotMeasureContest.0.BallotTitle.3.Text.0.language"
        "bs001 bs002 bs003" "VipObject.0.BallotMeasureContest.0.BallotSelectionIds.1"
        "fips" "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Type.0"
        "54" "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.6.ExternalIdentifier.0.Value.1")

      (is (= 30
             (count (korma/select postgres/xml-tree-values
                      (korma/where {:results_id (:import-id out-ctx)}))))))))
