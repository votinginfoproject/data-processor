(ns vip.data-processor.db.statistics-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.statistics :refer :all]))

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
