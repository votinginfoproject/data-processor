(ns vip.data-processor.db.translations.v5-1.ballot-measure-contests-test
  (:require [clojure.test :refer [deftest testing is]]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-1.ballot-measure-contests :as bmc]))

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
