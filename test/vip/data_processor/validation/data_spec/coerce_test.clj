(ns vip.data-processor.validation.data-spec.coerce-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec.coerce :refer :all]))

(deftest coerce-integer-test
  (testing "returns the number if passed an integer"
    (are [n] (= n (coerce-integer n))
      1
      0
      1000
      12))
  (testing "parses strings and returns integers"
    (are [s n] (= n (coerce-integer s))
      "1" 1
      "0" 0
      "1234" 1234
      "8" 8))
  (testing "returns nil for strings not parsable as integers"
    (are [s] (nil? (coerce-integer s))
      "nope"
      "not a number"
      "1x3"
      "eleven"))
  (testing "returns nil for anything else"
    (are [v] (nil? (coerce-integer v))
      nil
      :symbol
      'keyword
      {:map 1}
      #{2 3 5})))
