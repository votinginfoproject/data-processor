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
          ctx (merge {:input [(io/as-file (io/resource "election.txt"))]}
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
  (testing "with no election.txt file the ctx includes an error"
    (let [db (sqlite/temp-db "no-load-elections-test")
          ctx (merge {:input []} db)
          out-ctx (load-elections (assoc ctx :input []))]
      (is (empty? (korma/select (get-in out-ctx [:tables :elections]))))
      (is (get-in out-ctx [:errors :load-elections])))))
