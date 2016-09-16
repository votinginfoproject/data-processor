(ns vip.data-processor.validation.db-test
  (:require [vip.data-processor.validation.db :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-no-duplicated-ids-test
  (testing "finds duplicated ids across CSVs and errors"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["duplicate-ids/contest.txt"
                                          "duplicate-ids/candidate.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicate-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :import
                            :identifier 5882300
                            :error-type :duplicate-ids
                            :error-value '("contests" "candidates")}))
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :import
                             :identifier 7
                             :error-type :duplicate-ids}))))

(deftest validate-no-duplicated-rows-test
  (testing "finds possibly duplicated rows in a table and warns"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["duplicate-rows/candidate.txt"
                                          "duplicate-rows/ballot_candidate.txt"
                                          "duplicate-rows/ballot.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicate-ids" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (doseq [id [3100047456984 3100047456989 3100047466988 3100047466990]]
        (is (contains-error? errors
                             {:severity :warnings
                              :scope :candidates
                              :identifier id
                              :error-type :duplicate-rows})))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :ballot-candidates
                            :identifier nil
                            :error-type :duplicate-rows
                            :error-value {:candidate_id 3100047456987
                                          :ballot_id 410004745}}))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :ballot-candidates
                            :identifier nil
                            :error-type :duplicate-rows
                            :error-value {:candidate_id 3100047466988
                                          :ballot_id 410004746}}))
      (testing "does not add errors for ballots"
        (doseq [id [1 2 3]]
          (assert-no-problems-2 errors
                                {:severity :warnings
                                 :scope :ballots
                                 :identifier id
                                 :error-type :duplicate-rows}))))))

(deftest validate-one-record-limit-test
  (testing "validates that only one row exists in certain files"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["bad-number-of-rows/election.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-one-record-limit]}
                     (sqlite/temp-db "too-many-records" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :elections
                            :identifier :global
                            :error-type :row-constraint
                            :error-value "File needs to contain exactly one row."})))))

(deftest validate-references-test
  (testing "finds bad references"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["bad-references/ballot.txt"
                                          "bad-references/referendum.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-references]}
                     (sqlite/temp-db "bad-references" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :ballots
                            :identifier 41100369
                            :error-type :unmatched-reference
                            :error-value {"referendum_id" 123456789}})))))

(deftest validate-no-unreferenced-rows-test
  (testing "finds rows not referenced"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["unreferenced-rows/ballot.txt"
                                          "unreferenced-rows/candidate.txt"
                                          "unreferenced-rows/contest.txt"
                                          "unreferenced-rows/ballot_candidate.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (are [scope id]
          (is (contains-error? errors
                               {:severity :warnings
                                :scope scope
                                :identifier id
                                :error-type :unreferenced-row}))
        :ballots 2
        :ballots 3
        :candidates 13
        :candidates 14)
      (testing "except for contests"
        (assert-no-problems-2 errors
                              {:severity :warnings
                               :scope :contests
                               :identifier 100
                               :error-type :unreferenced-row})))))
