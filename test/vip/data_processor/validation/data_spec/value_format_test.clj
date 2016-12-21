(ns vip.data-processor.validation.data-spec.value-format-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec.value-format :refer :all]))

(deftest no-negative-house-numbers
  (let [not-negative-house-number (:check not-negative-integer)]
    (testing "validates false on negative numbers"
      (is (not-negative-house-number "1"))
      (is (not-negative-house-number "2"))
      (is (not (not-negative-house-number "-1")))
      (is (not (not-negative-house-number "-2")))
      (is (not (not-negative-house-number "abc"))))))

(deftest test-all-digits
  (let [all-digits-regex (:check all-digits)]
    (testing "matches digit values"
      (are [s] (re-matches all-digits-regex s)
           "123"
           "456"))))


(deftest electoral-district-type-regex-test
  (let [electoral-district-type-regex (:check electoral-district-type)]
    (testing "matches a lot of expected values"
      (are [s] (re-matches electoral-district-type-regex s)
        "state"
        "statewide"
        "STATEWIDE"
        "u.s. senate"
        "us rep"
        "u.s. house"
        "congressional"
        "state senate"
        "ut senate"
        "house"
        "county"
        "countywide"
        "school member district"
        "town"
        "township"
        "sanitary"))
    (testing "does not match any random string"
      (are [s] (nil? (re-matches electoral-district-type-regex s))
        "parliament"
        "prom court"
        "Kang v Kodos"))))

(deftest phone-regex-test
  (let [phone-regex (:check phone)]
    (testing "matches a lot of expected values"
      (are [s] (re-find phone-regex s)
        "(555) 555-5555"
        "555-555-5555"
        "555.555.5555"
        "+55 555 555 5555"
        "(555) 555 5555 x123"
        "Call this number: (555) 555 5555 x123"))
    (testing "does not match nonsense"
      (are [s] (nil? (re-find phone-regex s))
        "123456"
        "call me"
        "Call this number: (555)"))))
