(ns vip.data-processor.validation.data-spec-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec.value-format :as format]
            [vip.data-processor.db.sqlite :as sqlite]))

(deftest coerce-rows-test
  (let [rows [{:id "1" :race "Mayor" :partisan 0 :date "2015-08-01"}
              {:id 2 :race "Republican Primary" :partisan 1 :date "2015-08-15"}
              {:id "3" :race "Best Hot Dog"}]]
    (testing "without columns to coerce, does nothing"
      (let [cols [{:name "id"}
                  {:name "race"}
                  {:name "partisan"}
                  {:name "date"}]
            result (coerce-rows cols rows)]
        (is (= result rows))))
    (testing "with coercions, coerces rows"
      (let [cols [{:name "id" :coerce coerce-integer}
                  {:name "race"}
                  {:name "partisan" :coerce coerce-boolean}
                  {:name "date" :coerce coerce-date}]
            result (coerce-rows cols rows)]
        (is (= (map :id result) [1 2 3]))
        (is (= (map :race result) (map :race rows)))
        (is (= (map :partisan result) [false true nil]))
        (is (= (map (comp class :date) result) [java.sql.Date java.sql.Date nil]))))))

(deftest create-format-rule-test
  (let [column "id"
        filename "test.txt"
        line-number 7
        ctx {}]
    (testing "required column"
      (let [format-rule (create-format-rule filename {:name column :required true :format format/all-digits})]
        (testing "if the required column is missing, adds a fatal error"
          (let [result-ctx (format-rule ctx {column ""} line-number)]
            (is (= (ffirst (get-in result-ctx [:fatal filename line-number]))
                column))))
        (testing "if the column doesn't have the right format, adds an error"
          (let [result-ctx (format-rule ctx {column "asdf"} line-number)]
            (is (= (ffirst (get-in result-ctx [:errors filename line-number]))
                column))))
        (testing "if the required column is there and matches the format, is okay"
          (let [result-ctx (format-rule ctx {column "1234"} line-number)]
            (is (= ctx result-ctx))))))
    (testing "optional column"
      (let [format-rule (create-format-rule filename {:name column :format format/all-digits})]
        (testing "if it's not there, it's okay"
          (let [result-ctx (format-rule ctx {} line-number)]
            (is (= ctx result-ctx))))
        (testing "if it is there"
          (testing "it matches the format, everything's okay"
            (let [result-ctx (format-rule ctx {column "1234"} line-number)]
              (is (= ctx result-ctx))))
          (testing "it doesn't match the format, you get an error"
            (let [result-ctx (format-rule ctx {column "asdf"} line-number)]
              (is (= (ffirst (get-in result-ctx [:errors filename line-number]))
                  column)))))))
    (testing "a check that is a list of options"
      (let [format-rule (create-format-rule filename {:name column :format format/yes-no})]
        (testing "matches"
          (is (= ctx (format-rule ctx {column "yes"} line-number)))
          (is (= ctx (format-rule ctx {column "no"} line-number))))
        (testing "non-matches"
          (is (= (ffirst (get-in (format-rule ctx {column "YEP!"} line-number) [:errors filename line-number]))
              column))
          (is (= (ffirst (get-in (format-rule ctx {column "no way"} line-number) [:errors filename line-number]))
                 column)))
        (testing "matches of all kinds of cases"
          (is (= ctx (format-rule ctx {column "YES"} line-number)))
          (is (= ctx (format-rule ctx {column "NO"} line-number)))
          (is (= ctx (format-rule ctx {column "Yes"} line-number))))))
    (testing "a check that is a function"
      (let [palindrome? (fn [v] (= v (clojure.string/reverse v)))
            format-rule (create-format-rule filename {:name column :format {:check palindrome? :message "Not a palindrome"}})]
        (testing "matches"
          (is (= ctx (format-rule ctx {column "able was I ere I saw elba"} line-number)))
          (is (= ctx (format-rule ctx {column "racecar"} line-number))))
        (testing "non-matches"
          (is (= (ffirst (get-in (format-rule ctx {column "abcdefg"} line-number) [:errors filename line-number]))
              column))
          (is (= (ffirst (get-in (format-rule ctx {column "cleveland"} line-number) [:errors filename line-number]))
              column)))))
    (testing "if there's no check function, everything is okay"
      (let [format-rule (create-format-rule filename {:name column})]
        (is (= ctx (format-rule ctx {column "hi"} line-number)))
        (is (= ctx (format-rule ctx {} line-number)))))))

(deftest invalid-utf-8-test
  (testing "marks any value with a Unicode replacement character as invalid UTF-8 because that's what we assume we get"
    (let [ctx (merge {:input (csv-inputs ["invalid-utf8/source.txt"])
                    :data-specs data-specs}
                   (sqlite/temp-db "invalid-utf-8"))
          out-ctx (csv/load-csvs ctx)]
    (testing "reports errors for values with the Unicode replacement character"
      (is (= (get-in out-ctx [:errors :sources 2 "name"])
             ["Is not valid UTF-8."]))))))

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
