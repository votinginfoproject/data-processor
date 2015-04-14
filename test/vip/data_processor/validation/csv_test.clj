(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.data-spec :refer [data-specs]]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma])
  (:import [java.io File]))

(deftest remove-bad-filenames-test
  (let [bad-filenames [(File. "/data/BAD_FILE_NAME")
                       (File. "/data/BAD_FILE_NAME_2")]
        good-filenames (map #(File. (str "/data/" %)) csv-filenames)
        ctx {:input good-filenames}]
    (testing "with good filenames passes the context through"
      (is (= ctx (remove-bad-filenames ctx))))
    (testing "with bad filenames removes the bad files and warns"
      (let [ctx (update ctx :input (partial concat bad-filenames))
            out-ctx (remove-bad-filenames ctx)]
        (is (get-in out-ctx [:warnings :validate-filenames]))
        (is (not-every? good-filename? (:input ctx)))
        (is (every? good-filename? (:input out-ctx)))))))

(deftest missing-files-test
  (testing "reports errors or warnings when certain files are missing"
    (let [ctx {:input (csv-inputs ["full-good-run/source.txt"])}
          out-ctx (-> ctx
                      ((error-on-missing-file "election.txt"))
                      ((error-on-missing-file "source.txt"))
                      ((warn-on-missing-file "state.txt")))]
      (is (get-in out-ctx [:errors "election.txt"]))
      (is (get-in out-ctx [:warnings "state.txt"]))
      (is (not (contains? (:errors out-ctx) "source.txt"))))))

(deftest csv-loader-test
  (testing "ignores unknown columns"
    (let [ctx (merge {:input (csv-inputs ["bad-columns/state.txt"])
                      :data-specs data-specs}
                     (sqlite/temp-db "ignore-columns-test"))
          out-ctx (load-csvs ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (get-in out-ctx [:warnings "state.txt" :extraneous-headers])))))
  (testing "requires a header row"
    (let [ctx (merge {:input (csv-inputs ["no-header-row/ballot.txt"])
                      :data-specs data-specs}
                     (sqlite/temp-db "no-headers-test"))
          out-ctx (load-csvs ctx)]
      (is (= "No header row" (get-in out-ctx [:critical "ballot.txt" :headers]))))))

(deftest missing-required-columns-test
  (let [ctx (merge {:input (csv-inputs ["missing-required-columns/contest.txt"])
                    :data-specs data-specs}
                   (sqlite/temp-db "missing-required-columns"))
        out-ctx (load-csvs ctx)]
    (testing "adds a critical error for contest.txt"
      (is (get-in out-ctx [:critical "contest.txt"])))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest number-of-values-in-a-row-test
  (let [ctx (merge {:input (csv-inputs ["bad-number-of-values/contest.txt"])
                    :data-specs data-specs}
                   (sqlite/temp-db "bad-number-of-values"))
        out-ctx (load-csvs ctx)]
    (testing "reports critical errors for rows with wrong number of values"
      (is (get-in out-ctx [:critical "contest.txt" 3 "Number of values"]))
      (is (get-in out-ctx [:critical "contest.txt" 5 "Number of values"])))))
