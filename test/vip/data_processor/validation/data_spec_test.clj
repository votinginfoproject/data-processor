(ns vip.data-processor.validation.data-spec-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec.coerce :as coerce]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
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
      (let [cols [{:name "id" :coerce coerce/coerce-integer}
                  {:name "race"}
                  {:name "partisan" :coerce coerce/coerce-boolean}
                  {:name "date" :coerce coerce/coerce-date}]
            result (coerce-rows cols rows)]
        (is (= (map :id result) [1 2 3]))
        (is (= (map :race result) (map :race rows)))
        (is (= (map :partisan result) [false true nil]))
        (is (= (map (comp class :date) result) [java.sql.Date java.sql.Date nil]))))))

(deftest create-format-rule-test
  (let [column "name"
        filename "test.txt"
        id "3"
        line-number 7
        ctx {}]
    (testing "with an id, uses the id"
      (testing "required column"
        (let [format-rule (create-format-rule filename {:name column :required :critical :format format/all-digits})]
          (testing "if the required column is missing, adds an error of the specified severity"
            (let [result-ctx (format-rule ctx {column "" "id" id} line-number)]
              (is (= (ffirst (get-in result-ctx [:critical filename id]))
                     column))))
          (testing "if the column doesn't have the right format, adds an error"
            (let [result-ctx (format-rule ctx {column "asdf" "id" id} line-number)]
              (is (= (ffirst (get-in result-ctx [:errors filename id]))
                     column))))
          (testing "if the required column is there and matches the format, is okay"
            (let [result-ctx (format-rule ctx {column "1234" "id" id} line-number)]
              (is (= ctx result-ctx))))))
      (testing "optional column"
        (let [format-rule (create-format-rule filename {:name column :format format/all-digits})]
          (testing "if it's not there, it's okay"
            (let [result-ctx (format-rule ctx {} line-number)]
              (is (= ctx result-ctx))))
          (testing "if it is there"
            (testing "it matches the format, everything's okay"
              (let [result-ctx (format-rule ctx {column "1234" "id" id} line-number)]
                (is (= ctx result-ctx))))
            (testing "it doesn't match the format, you get an error"
              (let [result-ctx (format-rule ctx {column "asdf" "id" id} line-number)]
                (is (= (ffirst (get-in result-ctx [:errors filename id]))
                       column)))))))
      (testing "a check that is a list of options"
        (let [format-rule (create-format-rule filename {:name column :format format/yes-no})]
          (testing "matches"
            (is (= ctx (format-rule ctx {column "yes" "id" id} line-number)))
            (is (= ctx (format-rule ctx {column "no" "id" id} line-number))))
          (testing "non-matches"
            (is (= (ffirst (get-in (format-rule ctx {column "YEP!" "id" id} line-number) [:errors filename id]))
                   column))
            (is (= (ffirst (get-in (format-rule ctx {column "no way" "id" id} line-number) [:errors filename id]))
                   column)))
          (testing "matches of all kinds of cases"
            (is (= ctx (format-rule ctx {column "YES" "id" id} line-number)))
            (is (= ctx (format-rule ctx {column "NO" "id" id} line-number)))
            (is (= ctx (format-rule ctx {column "Yes" "id" id} line-number))))))
      (testing "a check that is a function"
        (let [palindrome? (fn [v] (= v (clojure.string/reverse v)))
              format-rule (create-format-rule filename {:name column :format {:check palindrome? :message "Not a palindrome"}})]
          (testing "matches"
            (is (= ctx (format-rule ctx {column "able was I ere I saw elba" "id" id} line-number)))
            (is (= ctx (format-rule ctx {column "racecar" "id" id} line-number))))
          (testing "non-matches"
            (is (= (ffirst (get-in (format-rule ctx {column "abcdefg" "id" id} line-number) [:errors filename id]))
                   column))
            (is (= (ffirst (get-in (format-rule ctx {column "cleveland" "id" id} line-number) [:errors filename id]))
                   column)))))
      (testing "if there's no check function, everything is okay"
        (let [format-rule (create-format-rule filename {:name column})]
          (is (= ctx (format-rule ctx {column "hi" "id" id} line-number)))
          (is (= ctx (format-rule ctx {"id" id} line-number))))))
    (testing "without an id, uses the line number"
      (testing "required column"
        (let [format-rule (create-format-rule filename {:name column :required :fatal :format format/all-digits})]
          (testing "if the required column is missing, adds an error of the specified severity"
            (let [result-ctx (format-rule ctx {column ""} line-number)]
              (is (= (ffirst (get-in result-ctx [:fatal filename line-number]))
                     column))))
          (testing "if the column doesn't have the right format, adds an error"
            (let [result-ctx (format-rule ctx {column "asdf"} line-number)]
              (is (= (ffirst (get-in result-ctx [:errors filename line-number]))
                     column))))
          (testing "if the required column is there and matches the format, is okay"
            (let [result-ctx (format-rule ctx {column "1234"} line-number)]
              (is (= ctx result-ctx)))))))))

(deftest invalid-utf-8-test
  (testing "marks any value with a Unicode replacement character as invalid UTF-8 because that's what we assume we get"
    (let [ctx (merge {:input (csv-inputs ["invalid-utf8/source.txt"])
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "invalid-utf-8" "3.0"))
          out-ctx (csv/load-csvs ctx)]
      (testing "reports errors for values with the Unicode replacement character"
        (is (= (get-in out-ctx [:errors :sources "1" "name"])
               ["Is not valid UTF-8."]))))))
