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
