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
