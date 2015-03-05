(ns vip.data-processor.db.sqlite-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.sqlite :refer :all]))

(deftest chunk-rows-test
  (let [n 7
        rows [{:first "George" :last "Washington" :start 1789}
              {:first "John"   :last "Adams"      :start 1797}
              {:first "Thomas" :last "Jefferson"  :start 1801}
              {:first "James"  :last "Madison"    :start 1809}
              {:first "James"  :last "Monroe"     :start 1817}]
        chunked (chunk-rows rows n)]
    (testing "returns a lazy sequence"
      (is (not (realized? chunked))))
    (testing "contains every row in the same order"
      (is (= rows (apply concat chunked))))
    (testing "no chunk has more than n keys"
      (doseq [n (range 3 17)]
        (let [chunked (chunk-rows rows n)
              key-count (fn [coll] (apply + (map count coll)))
              no-more-than-n-keys? (fn [coll] (>= n (key-count coll)))]
          (is (every? no-more-than-n-keys? chunked)))))))
