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
               :fatal {:this-one-doesnt-have-that-table {:global {:not-there ["So it will contribute 0 to the count"]}}}}]
      (is (= 9 (error-count table-name ctx))))))
