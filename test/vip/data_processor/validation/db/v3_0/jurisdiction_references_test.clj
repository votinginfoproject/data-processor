(ns vip.data-processor.validation.db.v3-0.jurisdiction-references-test
  (:require [vip.data-processor.validation.db.v3-0.jurisdiction-references :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]))

(deftest validate-jurisdiction-references-test
  (testing "finds bad jurisdiction references"
    (let [ctx (merge {:input (csv-inputs ["bad-references/ballot_line_result.txt"
                                          "bad-references/state.txt"
                                          "bad-references/locality.txt"
                                          "bad-references/precinct.txt"
                                          "bad-references/precinct_split.txt"
                                          "bad-references/electoral_district.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-jurisdiction-references]}
                     (sqlite/temp-db "bad-jurisdiction-references" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= 8 (-> out-ctx
                   (get-in [:errors :ballot-line-results 100 :unmatched-reference])
                   first
                   :jurisdiction_id)))
      (is (= 800 (-> out-ctx
                     (get-in [:errors :ballot-line-results 101 :unmatched-reference])
                     first
                     :jurisdiction_id)))
      (assert-error-format out-ctx))))
