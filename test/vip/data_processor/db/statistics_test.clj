(ns vip.data-processor.db.statistics-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.statistics :refer :all]))

(deftest error-count-test
  (testing "reports appropriate total numbers for our one error format"
    (let [table-name :contests
          ctx {:warnings {table-name {:global {:missing-header ["Header missing"]
                                               :too-long ["Table seems way too long"]}}}
               :errors {table-name {3 {:bad-names [{"Charleeee" "Too many E's"}
                                                   {"" "Too short"}
                                                   {"Christopher" "Already taken"}]}}}
               :critical {table-name {4 {:just-some-more [1 2 3 4]}}}
               :fatal {:this-one-doesnt-have-that-table
                       {:global {:not-there ["So it will contribute 0 to the count"]}}}}]
      (is (= 9 (error-count table-name ctx))))))

(deftest complete-test
  (testing "rows less errors gives the success percentage"
    (let [rows 400 errors 24]
      (is (= 94 (complete rows errors)))))

  (testing "when there are no rows and no errors, we have 100% completionn"
    (let [rows 0 errors 0]
      (is (= 100 (complete rows errors)))))

  (testing "when there are more errors than rows, we have 0% completion"
    (let [rows 0 errors 1]
      (is (= 0 (complete rows errors))))))
