(ns vip.data-processor.validation.db-test
  (:require [vip.data-processor.validation.db :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]))

(deftest validate-no-duplicated-ids-test
  (testing "finds duplicated ids across CSVs and errors"
    (let [ctx (merge {:input (csv-inputs ["duplicate-ids/contest.txt"
                                          "duplicate-ids/candidate.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-ids]}
                     (sqlite/temp-db "duplicate-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= #{"contests" "candidates"}
             (set (get-in out-ctx [:errors :import 5882300 :duplicate-ids]))
             (set (get-in out-ctx [:errors :import 8675309 :duplicate-ids]))))
      (is (not (get-in out-ctx [:errors :import 7 :duplicated-ids])))
      (assert-error-format out-ctx))))

(deftest validate-no-duplicated-rows-test
  (testing "finds possibly duplicated rows in a table and warns"
    (let [ctx (merge {:input (csv-inputs ["duplicate-rows/candidate.txt"
                                          "duplicate-rows/ballot_candidate.txt"
                                          "duplicate-rows/ballot.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-duplicated-rows]}
                     (sqlite/temp-db "duplicate-ids"))
          out-ctx (pipeline/run-pipeline ctx)]
      (doseq [id [3100047456984 3100047456989 3100047466988 3100047466990]]
        (is (get-in out-ctx [:warnings :candidates id :duplicate-rows])))
      (is (= #{{:candidate_id 3100047456987, :ballot_id 410004745} {:candidate_id 3100047466988, :ballot_id 410004746}}
             (set (get-in out-ctx [:warnings :ballot-candidates nil :duplicate-rows]))))
      (testing "does not add errors for ballots"
        (doseq [id [1 2 3]]
          (is (nil? (get-in out-ctx [:warnings :ballots id :duplicate-rows])))))
      (assert-error-format out-ctx))))

(deftest validate-one-record-limit-test
  (testing "validates that only one row exists in certain files"
    (let [ctx (merge {:input (csv-inputs ["bad-number-of-rows/election.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-one-record-limit]}
                     (sqlite/temp-db "too-many-records"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (get-in out-ctx [:errors :elections :global :row-constraint])
             ["File needs to contain exactly one row."]))
      (assert-error-format out-ctx))))

(deftest validate-references-test
  (testing "finds bad references"
    (let [ctx (merge {:input (csv-inputs ["bad-references/ballot.txt"
                                          "bad-references/referendum.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-references]}
                     (sqlite/temp-db "bad-references"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= 123456789 (-> out-ctx
                           (get-in [:errors :ballots 41100369 :unmatched-reference])
                           first
                           (get "referendum_id"))))
      (assert-error-format out-ctx))))

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
                     (sqlite/temp-db "bad-jurisdiction-references"))
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

(deftest validate-no-unreferenced-rows-test
  (testing "finds rows not referenced"
    (let [ctx (merge {:input (csv-inputs ["unreferenced-rows/ballot.txt"
                                          "unreferenced-rows/candidate.txt"
                                          "unreferenced-rows/contest.txt"
                                          "unreferenced-rows/ballot_candidate.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-no-unreferenced-rows]}
                     (sqlite/temp-db "unreferenced-rows"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:warnings :ballots 2 :unreferenced-row]))
      (is (get-in out-ctx [:warnings :ballots 3 :unreferenced-row]))
      (is (get-in out-ctx [:warnings :candidates 13 :unreferenced-row]))
      (is (get-in out-ctx [:warnings :candidates 14 :unreferenced-row]))
      (testing "except for contests"
        (is (nil? (get-in out-ctx [:warnings :contests 100 :unreferenced-row]))))
      (assert-error-format out-ctx))))

(deftest validate-no-overlapping-street-segments-test
  (let [ctx (merge {:input (csv-inputs ["overlapping-street-segments/street_segment.txt"])
                    :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                               csv/load-csvs
                               validate-no-overlapping-street-segments]}
                   (sqlite/temp-db "overlapping-street-segments"))
        out-ctx (pipeline/run-pipeline ctx)]
    (is (= '(12) (get-in out-ctx [:errors :street-segments 11 :overlaps])))
    (is (= '(14) (get-in out-ctx [:errors :street-segments 13 :overlaps])))
    (is (= #{16 17} (set (get-in out-ctx [:errors :street-segments 15 :overlaps]))))
    (is (= '(19) (get-in out-ctx [:errors :street-segments 18 :overlaps])))
    (is (= '(21) (get-in out-ctx [:errors :street-segments 20 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 22 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 23 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 24 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 25 :overlaps])))
    (is (not (get-in out-ctx [:errors :street-segments 26 :overlaps])))
    (is (= '(28) (get-in out-ctx [:errors :street-segments 27 :overlaps])))
    (assert-error-format out-ctx)))

(deftest validate-election-administration-addresses-test
  (testing "errors are returned if either the physical or mailing address is incomplete"
    (let [ctx (merge {:input (csv-inputs ["bad-election-administration-addresses/election_administration.txt"])
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-election-administration-addresses]}
                     (sqlite/temp-db "incomplete-addresses"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors :election-administrations 99990
                           :incomplete-physical-address]))
      (is (get-in out-ctx [:errors :election-administrations 99991
                           :incomplete-mailing-address]))
      (assert-error-format out-ctx))))
