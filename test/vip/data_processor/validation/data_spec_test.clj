(ns vip.data-processor.validation.data-spec-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec.coerce :as coerce]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.data-spec.value-format :as format]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.core.async :as a]))

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
        (is (= (map (comp class :date) result) [java.sql.Date java.sql.Date nil]))))
    (testing "it works with string keys also"
      (let [rows [{"id" "100" "race" "Mayor" "partisan" 0 "date" "2016-11-08"}]
            cols [{:name "id" :coerce coerce/coerce-integer}
                  {:name "race"}
                  {:name "partisan" :coerce coerce/coerce-boolean}
                  {:name "date" :coerce coerce/coerce-date}]
            result (first (coerce-rows cols rows))]
        (is (= (get result "id") 100))))))

(deftest create-format-rule-test
  (let [column "name"
        filename "test.txt"
        id "3"
        line-number 7
        format {:check #"\A\d+\z"
                :message "Invalid data type"}]
    (testing "with an id, uses the id"
      (testing "required column"
        (let [format-rule (create-format-rule filename {:name column :required :critical :format format})]
          (testing "if the required column is missing, adds an error of the specified severity"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :critical
                                    :scope filename
                                    :identifier id
                                    :error-type column}))))
          (testing "if the column doesn't have the right format, adds an error"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "asdf" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :errors
                                    :scope filename
                                    :identifier id
                                    :error-type column}))))
          (testing "if the required column is there and matches the format, is okay"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "1234" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (assert-no-problems errors {}))))))
      (testing "optional column"
        (let [format-rule (create-format-rule filename {:name column :format format})]
          (testing "if it's not there, it's okay"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {} line-number)
                  errors (all-errors errors-chan)]
              (is (assert-no-problems errors {}))))
          (testing "if it is there"
            (testing "it matches the format, everything's okay"
              (let [errors-chan (a/chan 100)
                    ctx {:errors-chan errors-chan}
                    result-ctx (format-rule ctx {column "1234" "id" id} line-number)
                    errors (all-errors errors-chan)]
                (is (assert-no-problems errors {}))))
            (testing "it doesn't match the format, you get an error"
              (let [errors-chan (a/chan 100)
                    ctx {:errors-chan errors-chan}
                    result-ctx (format-rule ctx {column "asdf" "id" id} line-number)
                    errors (all-errors errors-chan)]
                (is (contains-error? errors
                                     {:severity :errors
                                      :scope filename
                                      :identifier id
                                      :error-type column})))))))
      (testing "a check that is a list of options"
        (let [format-rule (create-format-rule filename {:name column :format format/yes-no})]
          (testing "matches"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "yes" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (assert-no-problems errors {})))
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "no" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (assert-no-problems errors {}))))
          (testing "non-matches"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "YEP!" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :errors
                                    :scope filename
                                    :identifier id
                                    :error-type column})))
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "no way" "id" id} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :errors
                                    :scope filename
                                    :identifier id
                                    :error-type column}))))
          (testing "matches of all kinds of cases"
            (are [value]
                (let [errors-chan (a/chan 100)
                      ctx {:errors-chan errors-chan}
                      result-ctx (format-rule ctx {column value "id" id} line-number)
                      errors (all-errors errors-chan)]
                  (is (assert-no-problems errors {})))
              "YES"
              "NO"
              "Yes"))))
      (testing "a check that is a function"
        (let [palindrome? (fn [v] (= v (clojure.string/reverse v)))
              format-rule (create-format-rule filename {:name column :format {:check palindrome? :message "Not a palindrome"}})]
          (testing "matches"
            (are [value]
                (let [errors-chan (a/chan 100)
                      ctx {:errors-chan errors-chan}
                      result-ctx (format-rule ctx {column value "id" id} line-number)
                      errors (all-errors errors-chan)]
                  (is (assert-no-problems errors {})))
              "able was I ere I saw elba"
              "racecar"))
          (testing "non-matches"
            (are [value]
                (let [errors-chan (a/chan 100)
                      ctx {:errors-chan errors-chan}
                      result-ctx (format-rule ctx {column value "id" id} line-number)
                      errors (all-errors errors-chan)]
                  (is (contains-error? errors
                                       {:severity :errors
                                        :scope filename
                                        :identifier id
                                        :error-type column})))
              "abcdefg"
              "cleveland")))
        (testing "with a severity key, uses that severity"
          (let [format-rule (create-format-rule filename
                                                {:name column
                                                 :format format/yes-no
                                                 :severity :warnings})
                errors-chan (a/chan 100)
                ctx {:errors-chan errors-chan}
                results-ctx (format-rule ctx {column "nyet" "id" id} line-number)
                errors (all-errors errors-chan)]
            (is (contains-error? errors
                                 {:severity :warnings
                                  :scope filename
                                  :identifier id
                                  :error-type column})))))
      (testing "if there's no check function, everything is okay"
        (let [format-rule (create-format-rule filename {:name column})]
          (are [value]
              (let [errors-chan (a/chan 100)
                    ctx {:errors-chan errors-chan}
                    result-ctx (format-rule ctx {column value "id" id} line-number)
                    errors (all-errors errors-chan)]
                (is (assert-no-problems errors {})))
            "hi"
            nil))))
    (testing "without an id, uses the line number"
      (testing "required column"
        (let [format-rule (create-format-rule filename {:name column :required :fatal :format format})]
          (testing "if the required column is missing, adds an error of the specified severity"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column ""} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :fatal
                                    :scope filename
                                    :identifier line-number
                                    :error-type column}))))
          (testing "if the column doesn't have the right format, adds an error"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "asdf"} line-number)
                  errors (all-errors errors-chan)]
              (is (contains-error? errors
                                   {:severity :errors
                                    :scope filename
                                    :identifier line-number
                                    :error-type column}))))
          (testing "if the required column is there and matches the format, is okay"
            (let [errors-chan (a/chan 100)
                  ctx {:errors-chan errors-chan}
                  result-ctx (format-rule ctx {column "1234"} line-number)
                  errors (all-errors errors-chan)]
              (assert-no-problems errors {}))))))
    (testing "with a severity set on the format"
      (let [format {:check #"\A\d+\z"
                    :message "Invalid data type"
                    :severity :fatal}
            format-rule (create-format-rule filename {:name column
                                                      :required :fatal
                                                      :format format})]
        (testing "not overridden creates errors at the format's severity"
          (let [errors-chan (a/chan 100)
                ctx {:errors-chan errors-chan}
                result-ctx (format-rule ctx {column "asdf"} line-number)
                errors (all-errors errors-chan)]
            (is (contains-error? errors
                                 {:severity :fatal
                                  :scope filename
                                  :identifier line-number
                                  :error-type column}))))
        (testing "overridden creates errors at the override severity"
          (let [format-rule (create-format-rule filename {:name column
                                                          :required :fatal
                                                          :format format
                                                          :severity :warnings})
                errors-chan (a/chan 100)
                ctx {:errors-chan errors-chan}
                result-ctx (format-rule ctx {column "asdf"} line-number)
                errors (all-errors errors-chan)]
            (is (contains-error? errors
                                 {:severity :warnings
                                  :scope filename
                                  :identifier line-number
                                  :error-type column}))))))))

(deftest invalid-utf-8-test
  (testing "marks any value with a Unicode replacement character as invalid UTF-8 because that's what we assume we get"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["invalid-utf8/source.txt"])
                      :errors-chan errors-chan
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "invalid-utf-8" "3.0"))
          out-ctx (csv/load-csvs ctx)
          errors (all-errors errors-chan)]
      (testing "reports errors for values with the Unicode replacement character"
        (is (contains-error? errors
                             {:severity :errors
                              :scope :sources
                              :identifier "1"
                              :error-type "name"
                              :error-value "Is not valid UTF-8."}))))))
