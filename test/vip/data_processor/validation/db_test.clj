(ns vip.data-processor.validation.db-test
  (:require [vip.data-processor.validation.db :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.java.io :as io]))

(deftest validate-no-duplicated-ids-test
  (testing "finds duplicated ids across CSVs and errors"
    (let [ctx (merge {:input [(io/as-file (io/resource "duplicate-ids/contest.txt"))
                              (io/as-file (io/resource "duplicate-ids/candidate.txt"))]
                      :pipeline [(csv/load-csvs csv/csv-specs)
                                 validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicate-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors "Duplicate IDs" 8675309]))
      (is (get-in out-ctx [:errors "Duplicate IDs" 5882300]))
      (is (not (get-in out-ctx [:errors "Duplicate IDs" 7]))))))
