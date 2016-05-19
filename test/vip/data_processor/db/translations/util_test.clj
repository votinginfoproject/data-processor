(ns vip.data-processor.db.translations.util-test
  (:require [clojure.test :refer [deftest testing is]]
            [vip.data-processor.db.translations.util :as util]))

(deftest index-generator-test
  (testing "a generator yields monotonically increasing values on each call"
    (let [idx (util/index-generator 13)
          values (into [] (take 5 (repeatedly idx)))]
      (is (= [13 14 15 16 17] values))
      (is (every? > values)))))

(deftest column->xml-elment-test
  (testing "column names are given as keywords"
    (is (= "Singleword"
           (util/column->xml-elment :singleword)))
    (is (= "CamelCaseFromSnakeCase"
           (util/column->xml-elment :camel_case_from_snake_case)))
    (is (= "No-hyphens-yet"
           (util/column->xml-elment :no-hyphens-yet)))))

(deftest path->simple-path-test
  (testing "a simple path is a path without index numbers"
    (is (= "VipObject.BallotMeasureContest"
           (util/path->simple-path "VipObject.0.BallotMeasureContest.0")))))

(deftest simple-value->ltree-test
  (testing "transforming a simple thing is simple"
    (let [row {:abbreviation "DR"}
          transform-fn (util/simple-value->ltree :abbreviation)
          idx-fn (util/index-generator 0)]
      (is (= (list
              {:path "VipObject.0.BallotMeasureContest.0.Abbreviation.0"
               :simple_path "VipObject.BallotMeasureContest.Abbreviation"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "DR"})
             (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row)))))

  (testing "alternate element names may be used where CSV and XML differ"
    (let [row {:abbreviation "JRA"}
          transform-fn (util/simple-value->ltree :abbreviation "TLA")
          idx-fn (util/index-generator 0)]
      (is (= (list
              {:path "VipObject.0.BallotMeasureContest.0.TLA.0"
               :simple_path "VipObject.BallotMeasureContest.TLA"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "JRA"})
             (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row))))))

(deftest internationalized-text->ltree-test
  (testing "internationalized text elements will create two rows"
    (let [row {:instructions "Pat your head and rub your belly"}
          transform-fn (util/internationalized-text->ltree :instructions)
          idx-fn (util/index-generator 0)]
      (is (= (list
              {:path "VipObject.0.BallotMeasureContest.0.Instructions.0.Text.0"
               :simple_path "VipObject.BallotMeasureContest.Instructions.Text"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "Pat your head and rub your belly"}
              {:path "VipObject.0.BallotMeasureContest.0.Instructions.0.Text.0.language"
               :simple_path "VipObject.BallotMeasureContest.Instructions.Text.language"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "en"})
             (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row))))))

(deftest external-identifiers->ltree-test
  (testing "external identifier elements will create three rows"
    (let [row {:external_identifier_type "Other"
               :external_identifier_othertype "ThisOtherType"
               :external_identifier_value "ThisOtherType's value"}
          transform-fn util/external-identifiers->ltree
          idx-fn (util/index-generator 0)]
      (is (= #{{:path "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.0.ExternalIdentifier.0.Type.0"
                :simple_path "VipObject.BallotMeasureContest.ExternalIdentifiers.ExternalIdentifier.Type"
                :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
                :value "Other"}
               {:path "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.0.ExternalIdentifier.0.OtherType.1"
                :simple_path "VipObject.BallotMeasureContest.ExternalIdentifiers.ExternalIdentifier.OtherType"
                :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
                :value "ThisOtherType"}
               {:path "VipObject.0.BallotMeasureContest.0.ExternalIdentifiers.0.ExternalIdentifier.0.Value.2"
                :simple_path "VipObject.BallotMeasureContest.ExternalIdentifiers.ExternalIdentifier.Value"
                :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
                :value "ThisOtherType's value"}}
             (set (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row)))))))
