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

  (testing "when a thing is empty, it is not included"
    (let [row {:abbreviation ""}
          transform-fn (util/simple-value->ltree :abbreviation)
          idx-fn (util/index-generator 0)]
      (is (nil? (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row)))
      (is (= 0 (idx-fn)))))

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
              {:path "VipObject.0.BallotMeasureContest.0.Instructions.0.Text.0.language"
               :simple_path "VipObject.BallotMeasureContest.Instructions.Text.language"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "en"}
              {:path "VipObject.0.BallotMeasureContest.0.Instructions.0.Text.0"
               :simple_path "VipObject.BallotMeasureContest.Instructions.Text"
               :parent_with_id "VipObject.0.BallotMeasureContest.0.id"
               :value "Pat your head and rub your belly"})
             (transform-fn idx-fn "VipObject.0.BallotMeasureContest.0" row))))))

(deftest external-identifiers->ltree-test
  (testing "when all three identifiers are missing, nothing happens"
    (let [row {:external_identifier_type ""
               :external_identifier_othertype ""
               :external_identifier_value ""}
          idx-fn (util/index-generator 0)]
      (is (nil? (util/external-identifiers->ltree idx-fn "VipObject.0.BallotMeasureContest.0" row)))))
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

(deftest latlng->ltree-test
  (testing "when latitude and longitude are missing, nothing happens"
    (let [row {:latitude ""
               :longitude ""
               :latlng_source "Optional source"}
          idx-fn (util/index-generator 0)]
      (is (nil? (util/latlng->ltree idx-fn "VipObject.0.PollingLocation.0" row)))))

  (testing "LatLng elements have two core components"
    (let [row {:latitude "38.0754627"
               :longitude "78.5014875"
               :latlng_source ""}
          transform-fn util/latlng->ltree
          idx-fn (util/index-generator 0)]
      (is (= #{{:path "VipObject.0.PollingLocation.0.LatLng.0.Latitude.0"
                :simple_path "VipObject.PollingLocation.LatLng.Latitude"
                :parent_with_id "VipObject.0.PollingLocation.0.id"
                :value "38.0754627"}
               {:path "VipObject.0.PollingLocation.0.LatLng.0.Longitude.1"
                :simple_path "VipObject.PollingLocation.LatLng.Longitude"
                :parent_with_id "VipObject.0.PollingLocation.0.id"
                :value "78.5014875"}}
             (set (transform-fn idx-fn "VipObject.0.PollingLocation.0" row))))))

  (testing "Source is included when present"
    (let [row {:latitude "38.0754627"
               :longitude "78.5014875"
               :latlng_source "Google Maps"}
          transform-fn util/latlng->ltree
          idx-fn (util/index-generator 0)]
      (is (= #{{:path "VipObject.0.PollingLocation.0.LatLng.0.Latitude.0"
                :simple_path "VipObject.PollingLocation.LatLng.Latitude"
                :parent_with_id "VipObject.0.PollingLocation.0.id"
                :value "38.0754627"}
               {:path "VipObject.0.PollingLocation.0.LatLng.0.Longitude.1"
                :simple_path "VipObject.PollingLocation.LatLng.Longitude"
                :parent_with_id "VipObject.0.PollingLocation.0.id"
                :value "78.5014875"}
               {:path "VipObject.0.PollingLocation.0.LatLng.0.Source.2"
                :simple_path "VipObject.PollingLocation.LatLng.Source"
                :parent_with_id "VipObject.0.PollingLocation.0.id"
                :value "Google Maps"}}
             (set (transform-fn idx-fn "VipObject.0.PollingLocation.0" row)))))))
