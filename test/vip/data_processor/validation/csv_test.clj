(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :refer :all]
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

(deftest load-elections-test
  (testing "with a valid election.txt file"
    (let [db (sqlite/temp-db "load-elections-test")
          ctx (merge {:input [(io/as-file (io/resource "full-good-run/election.txt"))]}
                     db)]
      (is (empty? (korma/select (get-in ctx [:tables :elections]))))
      (testing "inserts rows from the valid election.txt file"
        (let [out-ctx (load-elections ctx)]
          (is (= '({:id 5400} {:id 5401})
                 (korma/select (get-in out-ctx [:tables :elections])
                               (korma/fields :id))))
          (testing "no/yes values are turned into 0/1"
            (is (= '({:id 5400 :statewide 1} {:id 5401 :statewide 0})
                   (korma/select (get-in out-ctx [:tables :elections])
                                 (korma/fields :id :statewide)))))))))
  (testing "with no election.txt file, does nothing"
    (let [db (sqlite/temp-db "no-load-elections-test")
          ctx (merge {:input []} db)
          out-ctx (load-elections (assoc ctx :input []))]
      (is (empty? (korma/select (get-in out-ctx [:tables :elections])))))))

(deftest load-sources-test
  (testing "with a valid source.txt file"
    (let [db (sqlite/temp-db "load-sources-test")
          ctx (merge {:input [(io/as-file (io/resource "full-good-run/source.txt"))]}
                     db)]
      (is (empty? (korma/select (get-in ctx [:tables :sources]))))
      (testing "inserts rows from the valid source.txt file"
        (let [out-ctx (load-sources ctx)]
          (is (= '({:id 4400})
                 (korma/select (get-in out-ctx [:tables :sources])
                               (korma/fields :id))))))))
  (testing "with no source.txt file, does nothing"
    (let [db (sqlite/temp-db "no-load-sources-test")
          ctx (merge {:input []} db)
          out-ctx (load-sources (assoc ctx :input []))]
      (is (empty? (korma/select (get-in out-ctx [:tables :sources])))))))

(deftest load-states-test
  (testing "with a valid state.txt file"
    (let [db (sqlite/temp-db "load-states-test")
          ctx (merge {:input [(io/as-file (io/resource "full-good-run/state.txt"))]}
                     db)]
      (is (empty? (korma/select (get-in ctx [:tables :states]))))
      (testing "inserts rows from the valid state.txt file"
        (let [out-ctx (load-states ctx)]
          (is (= '({:id 1})
                 (korma/select (get-in out-ctx [:tables :states])
                               (korma/fields :id))))))))
  (testing "with no state.txt file, does nothing"
    (let [db (sqlite/temp-db "no-load-states-test")
          ctx (merge {:input []} db)
          out-ctx (load-states (assoc ctx :input []))]
      (is (empty? (korma/select (get-in out-ctx [:tables :states])))))))

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
  (testing "ignores unknown columns"
    (let [loader (csv-loader "state-with-bad-columns.txt" :states)
          ctx (merge {:input [(io/as-file (io/resource "state-with-bad-columns.txt"))]}
                     (sqlite/temp-db "ignore-columns-test"))
          out-ctx (loader ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states])))))))
