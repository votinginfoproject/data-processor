(ns vip.data-processor.validation.data-spec-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec :refer :all]))

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
