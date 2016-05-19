(ns vip.data-processor.db.translations.v5-0.ballot-measure-contests-test
  (:require [clojure.test :refer [deftest testing is]]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-0.ballot-measure-contests :as bmc]))

(def bmc
  {:ballot_title "BALLOT_TITLE",
   :pro_statement "PRO_STATEMENT",
   :ballot_sub_title "BALLOT_SUB_TITLE",
   :results_id 1,
   :con_statement "CON_STATEMENT",
   :name "NAME",
   :external_identifier_value "EXTERNAL_IDENTIFIER_VALUE",
   :summary_text "SUMMARY_TEXT",
   :type "TYPE",
   :effect_of_abstain "EFFECT_OF_ABSTAIN",
   :electoral_district_id "ELECTORAL_DISTRICT_ID",
   :has_rotation "HAS_ROTATION",
   :full_text "FULL_TEXT",
   :id "bmc001",
   :external_identifier_othertype "EXTERNAL_IDENTIFIER_OTHERTYPE",
   :vote_variation "VOTE_VARIATION",
   :info_uri "INFO_URI",
   :other_type "OTHER_TYPE",
   :other_vote_variation "OTHER_VOTE_VARIATION",
   :passage_threshold "PASSAGE_THRESHOLD",
   :abbreviation "ABBREVIATION",
   :electorate_specification "ELECTORATE_SPECIFICATION",
   :external_identifier_type "EXTERNAL_IDENTIFIER_TYPE",
   :sequence_order "SEQUENCE_ORDER"})

(deftest abbreviation-ltree-test
  (let [idx-fn (util/index-generator 0)
        bmc-path "VipObject.0.BallotMeasureContest.0"
        index-path "VipObject.0.BallotMeasureContest.0.id"
        transform-fn (util/simple-value-ltree :abbreviation)]
    (testing "creating an abbreviation entity"
      (is (= (list
              {:simple_path "VipObject.BallotMeasureContest.Abbreviation"
               :path "VipObject.0.BallotMeasureContest.0.Abbreviation.0"
               :parent_with_id index-path
               :value "ABBREVIATION"})
             (transform-fn idx-fn bmc-path bmc))))))

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
