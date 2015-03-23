(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.csv.value-format :as format]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]
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
    (let [ctx {:input [(io/as-file (io/resource "full-good-run/source.txt"))]}
          out-ctx (-> ctx
                      ((error-on-missing-file "election.txt"))
                      ((error-on-missing-file "source.txt"))
                      ((warn-on-missing-file "state.txt")))]
      (is (get-in out-ctx [:errors "election.txt"]))
      (is (get-in out-ctx [:warnings "state.txt"]))
      (is (not (contains? (:errors out-ctx) "source.txt"))))))

(deftest csv-loader-test
  (testing "check for .txt file extension"
    (let [only-election-official-spec (filter #(= "election_official.txt" (:filename %))
                                              csv-specs)
          updated-spec (list (assoc (first only-election-official-spec)
                                    :filename "election_official.csv"))
          load-election-official (load-csvs updated-spec)
          ctx {:input [(File. "election_official.csv")]}
          out-ctx (load-election-official ctx)]
      (is "File is not a .txt file."
          (get-in out-ctx [:critical "election_official.csv" "File extension"]))))
  (testing "ignores unknown columns"
    (let [only-state-spec (filter #(= "state.txt" (:filename %)) csv-specs)
          load-state (load-csvs only-state-spec)
          ctx (merge {:input [(io/as-file (io/resource "bad-columns/state.txt"))]}
                     (sqlite/temp-db "ignore-columns-test"))
          out-ctx (load-state ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (get-in out-ctx [:warnings "state.txt" :extraneous-headers])))))
  (testing "requires a header row"
    (let [only-ballot-spec (filter #(= "ballot.txt" (:filename %)) csv-specs)
          load-ballots (load-csvs only-ballot-spec)
          ctx (merge {:input [(io/as-file (io/resource "no-header-row/ballot.txt"))]}
                     (sqlite/temp-db "no-headers-test"))
          out-ctx (load-ballots ctx)]
      (is (= "No header row" (get-in out-ctx [:critical "ballot.txt" :headers]))))))

(deftest missing-required-columns-test
  (let [ctx (merge {:input [(io/as-file (io/resource "missing-required-columns/contest.txt"))]}
                   (sqlite/temp-db "missing-required-columns"))
        only-contests-spec (filter #(= "contest.txt" (:filename %)) csv-specs)
        load-contests (load-csvs only-contests-spec)
        out-ctx (load-contests ctx)]
    (testing "adds a critical error for contest.txt"
      (is (get-in out-ctx [:critical "contest.txt"])))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest number-of-values-in-a-row-test
  (let [ctx (merge {:input [(io/as-file (io/resource "bad-number-of-values/contest.txt"))]}
                   (sqlite/temp-db "bad-number-of-values"))
        only-contests-spec (filter #(= "contest.txt" (:filename %)) csv-specs)
        load-contests (load-csvs only-contests-spec)
        out-ctx (load-contests ctx)]
    (testing "reports critical errors for rows with wrong number of values"
      (is (get-in out-ctx [:critical "contest.txt" 3 "Number of values"]))
      (is (get-in out-ctx [:critical "contest.txt" 5 "Number of values"])))))

(deftest create-format-rule-test
  (let [column "id"
        filename "test.txt"
        line-number 7
        ctx {}]
    (testing "required column"
      (let [format-rule (create-format-rule filename {:name column :required true :format format/all-digits})]
        (testing "if the required column is missing, adds a fatal error"
          (let [result-ctx (format-rule ctx {column ""} line-number)]
            (is (get-in result-ctx [:fatal filename line-number column]))))
        (testing "if the column doesn't have the right format, adds an error"
          (let [result-ctx (format-rule ctx {column "asdf"} line-number)]
            (is (get-in result-ctx [:errors filename line-number column]))))
        (testing "if the required column is there and matches the format, is okay"
          (let [result-ctx (format-rule ctx {column "1234"} line-number)]
            (is (= ctx result-ctx))))))
    (testing "optional column"
      (let [format-rule (create-format-rule filename {:name column :format format/all-digits})]
        (testing "if it's not there, it's okay"
          (let [result-ctx (format-rule ctx {} line-number)]
            (is (= ctx result-ctx))))
        (testing "if it is there"
          (testing "it matches the format, everything's okay"
            (let [result-ctx (format-rule ctx {column "1234"} line-number)]
              (is (= ctx result-ctx))))
          (testing "it doesn't match the format, you get an error"
            (let [result-ctx (format-rule ctx {column "asdf"} line-number)]
              (is (get-in result-ctx [:errors filename line-number column])))))))
    (testing "a check that is a list of options"
      (let [format-rule (create-format-rule filename {:name column :format format/yes-no})]
        (testing "matches"
          (is (= ctx (format-rule ctx {column "yes"} line-number)))
          (is (= ctx (format-rule ctx {column "no"} line-number))))
        (testing "non-matches"
          (is (get-in (format-rule ctx {column "YEP!"} line-number) [:errors filename line-number column]))
          (is (get-in (format-rule ctx {column "no way"} line-number) [:errors filename line-number column])))))
    (testing "a check that is a function"
      (let [palindrome? (fn [v] (= v (clojure.string/reverse v)))
            format-rule (create-format-rule filename {:name column :format {:check palindrome? :message "Not a palindrome"}})]
        (testing "matches"
          (is (= ctx (format-rule ctx {column "able was I ere I saw elba"} line-number)))
          (is (= ctx (format-rule ctx {column "racecar"} line-number))))
        (testing "non-matches"
          (is (get-in (format-rule ctx {column "abcdefg"} line-number) [:errors filename line-number column]))
          (is (get-in (format-rule ctx {column "cleveland"} line-number) [:errors filename line-number column])))))
    (testing "if there's no check function, everything is okay"
      (let [format-rule (create-format-rule filename {:name column})]
        (is (= ctx (format-rule ctx {column "hi"} line-number)))
        (is (= ctx (format-rule ctx {} line-number)))))))
