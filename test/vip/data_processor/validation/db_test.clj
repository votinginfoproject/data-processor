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
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicate-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors "Duplicate IDs" 8675309]))
      (is (get-in out-ctx [:errors "Duplicate IDs" 5882300]))
      (is (not (get-in out-ctx [:errors "Duplicate IDs" 7]))))))

(deftest validate-no-duplicated-rows-test
  (testing "finds possibly duplicated rows in a table and warns"
    (let [ctx (merge {:input [(io/as-file (io/resource "duplicate-rows/candidate.txt"))
                              (io/as-file (io/resource "duplicate-rows/ballot_candidate.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicate-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= #{3100047456984 3100047456989 3100047466988 3100047466990}
             (set (map :id (get-in out-ctx [:warnings "candidate.txt" :duplicated-rows])))))
      (is (= #{{:candidate_id 3100047456987, :ballot_id 410004745} {:candidate_id 3100047466988, :ballot_id 410004746}}
             (set (get-in out-ctx [:warnings "ballot_candidate.txt" :duplicated-rows])))))))

(deftest validate-one-record-limit-test
  (testing "validates that only one row exists in certain files"
    (let [ctx (merge {:input [(io/as-file
                                (io/resource "bad-number-of-rows/election.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-one-record-limit]}
                     (sqlite/temp-db "too-many-records"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (get-in out-ctx [:errors "election.txt" :row-constraint])
             "File needs to contain exactly one row.")))))

(deftest validate-references-test
  (testing "finds bad references"
    (let [ctx (merge {:input [(io/as-file (io/resource "bad-references/ballot.txt"))
                              (io/as-file (io/resource "bad-references/referendum.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-references]}
                     (sqlite/temp-db "bad-references"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= 1 (count (get-in out-ctx [:errors "ballot.txt" :reference-error "referendum_id"])))))))

(deftest validate-jurisdiction-references-test
  (testing "finds bad jurisdiction references"
    (let [ctx (merge {:input [(io/as-file (io/resource "bad-references/ballot_line_result.txt"))
                              (io/as-file (io/resource "bad-references/state.txt"))
                              (io/as-file (io/resource "bad-references/locality.txt"))
                              (io/as-file (io/resource "bad-references/precinct.txt"))
                              (io/as-file (io/resource "bad-references/precinct_split.txt"))
                              (io/as-file (io/resource "bad-references/electoral_district.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-jurisdiction-references]}
                     (sqlite/temp-db "bad-jurisdiction-references"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= [100 101] (map :id (get-in out-ctx [:errors "ballot_line_result.txt" :reference-error "jurisdiction_id"])))))))

(deftest validate-no-unreferenced-rows-test
  (testing "finds rows not referenced"
    (let [ctx (merge {:input [(io/as-file (io/resource "unreferenced-rows/ballot.txt"))
                              (io/as-file (io/resource "unreferenced-rows/candidate.txt"))
                              (io/as-file (io/resource "unreferenced-rows/ballot_candidate.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                                 csv/load-csvs
                                 validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= [2 3]
             (map :id (get-in out-ctx
                              [:warnings "ballot.txt" :unreferenced-rows]))))
      (is (= [13 14]
             (map :id (get-in out-ctx
                              [:warnings "candidate.txt" :unreferenced-rows])))))))

(deftest validate-no-overlapping-street-segments-test
  (let [ctx (merge {:input [(io/as-file (io/resource "overlapping-street-segments/street_segment.txt"))]
                    :pipeline [(csv/add-csv-specs csv/csv-specs)
                               csv/load-csvs
                               validate-no-overlapping-street-segments]}
                   (sqlite/temp-db "overlapping-street-segments"))
        out-ctx (pipeline/run-pipeline ctx)]
    (is (= #{#{11 12} #{13 14} #{15 16} #{17 18} #{19 20}}
           (get-in out-ctx [:errors "street_segment.txt" :overlaps])))))

(deftest validate-election-administration-addresses-test
  (testing "errors are returned if either the physical or mailing address is incomplete"
    (let [ctx (merge {:input [(io/as-file (io/resource "bad-election-administration-addresses/election_administration.txt"))]
                      :pipeline [(csv/add-csv-specs csv/csv-specs)
                               csv/load-csvs
                               validate-election-administration-addresses]}
                     (sqlite/temp-db "incomplete-addresses"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors "election_administration.txt"
                           :incomplete-physical-address]))
      (is (get-in out-ctx [:errors "election_administration.txt"
                           :incomplete-mailing-address])))))
