(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.csv.value-format :as format]
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
                      :csv-specs csv-specs}
                     (sqlite/temp-db "ignore-columns-test"))
          out-ctx (load-csvs ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (get-in out-ctx [:warnings "state.txt" :extraneous-headers])))))
  (testing "requires a header row"
    (let [ctx (merge {:input (csv-inputs ["no-header-row/ballot.txt"])
                      :csv-specs csv-specs}
                     (sqlite/temp-db "no-headers-test"))
          out-ctx (load-csvs ctx)]
      (is (= "No header row" (get-in out-ctx [:critical "ballot.txt" :headers]))))))

(deftest missing-required-columns-test
  (let [ctx (merge {:input (csv-inputs ["missing-required-columns/contest.txt"])
                    :csv-specs csv-specs}
                   (sqlite/temp-db "missing-required-columns"))
        out-ctx (load-csvs ctx)]
    (testing "adds a critical error for contest.txt"
      (is (get-in out-ctx [:critical "contest.txt"])))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest number-of-values-in-a-row-test
  (let [ctx (merge {:input (csv-inputs ["bad-number-of-values/contest.txt"])
                    :csv-specs csv-specs}
                   (sqlite/temp-db "bad-number-of-values"))
        out-ctx (load-csvs ctx)]
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

(deftest invalid-utf-8-test
  (testing "marks any value with a Unicode replacement character as invalid UTF-8 because that's what we assume we get"
    (let [ctx (merge {:input (csv-inputs ["invalid-utf8/source.txt"])
                    :csv-specs csv-specs}
                   (sqlite/temp-db "invalid-utf-8"))
          out-ctx (load-csvs ctx)]
    (testing "reports errors for values with the Unicode replacement character"
      (is (= (get-in out-ctx [:errors "source.txt" 2 "name"])
             "Is not valid UTF-8."))))))
