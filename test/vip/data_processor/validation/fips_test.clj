(ns vip.data-processor.validation.fips-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.fips :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.java.io :as io]))

(deftest validate-valid-source-vip-id-test
  (testing "adds an error if the source's vip_id is bad"
    (let [ctx (merge {:input [(io/as-file (io/resource "invalid-source-vip-id/source.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-valid-source-vip-id]}
                     (sqlite/temp-db "invalid-source-vip-id"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors :source :invalid-vip-id])))))
