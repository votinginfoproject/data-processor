(ns vip.data-processor.validation.db.v3-0.jurisdiction-references-test
  (:require [vip.data-processor.validation.db.v3-0.jurisdiction-references :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-jurisdiction-references-test
  (testing "finds bad jurisdiction references"
    (let [errors-chan (a/chan 100)
          ctx (merge {:csv-source-file-paths
                      (csv-inputs ["bad-references/ballot_line_result.txt"
                                   "bad-references/state.txt"
                                   "bad-references/locality.txt"
                                   "bad-references/precinct.txt"
                                   "bad-references/precinct_split.txt"
                                   "bad-references/electoral_district.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-jurisdiction-references]}
                     (sqlite/temp-db "bad-jurisdiction-references" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ballot-line-results
                            :identifier 100
                            :error-type :unmatched-reference
                            :error-value {:jurisdiction_id 8}}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ballot-line-results
                            :identifier 101
                            :error-type :unmatched-reference
                            :error-value {:jurisdiction_id 800}})))))
