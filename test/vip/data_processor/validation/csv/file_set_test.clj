(ns vip.data-processor.validation.csv.file-set-test
  (:require [vip.data-processor.validation.csv.file-set :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [clojure.test :refer :all]
            [clojure.core.async :as a])
  (:import [java.nio.file Paths]))

(deftest validate-dependencies-test
  (let [dependencies (build-dependencies
                      "ballot.txt" (and "candidate.txt"
                               (or "precinct.txt" "election.txt"))
                      "precinct.txt" "source.txt")
        validator (validate-dependencies dependencies)]
    (testing "does not add errors when dependencies are met"
      (let [errors-chan (a/chan 100)
            ctx {:csv-source-file-paths
                 [(Paths/get "ballot.txt" (into-array String []))
                  (Paths/get "candidate.txt" (into-array String []))
                  (Paths/get "election.txt" (into-array String []))]
                 :errors-chan errors-chan
                 :data-specs v3-0/data-specs}
            out-ctx (validator ctx)
            errors (all-errors errors-chan)]
        (assert-no-problems errors {}))
      (testing "with sub-dependencies"
        (let [errors-chan (a/chan 100)
              ctx {:csv-source-file-paths
                   [(Paths/get "ballot.txt" (into-array String []))
                    (Paths/get "candidate.txt" (into-array String []))
                    (Paths/get "precinct.txt" (into-array String []))
                    (Paths/get "source.txt" (into-array String []))]
                   :errors-chan errors-chan
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)
              errors (all-errors errors-chan)]
          (assert-no-problems errors {}))))
    (testing "adds errors when dependencies are not met"
      (let [errors-chan (a/chan 100)
            ctx {:csv-source-file-paths
                 [(Paths/get "ballot.txt" (into-array String []))
                  (Paths/get "precinct.txt" (into-array String []))]
                 :errors-chan errors-chan
                 :data-specs v3-0/data-specs}
            out-ctx (validator ctx)
            errors (all-errors errors-chan)]
        (is (contains-error? errors
                             {:scope :ballots
                              :identifier :global
                              :error-type :missing-dependency}))
        (is (contains-error? errors
                             {:scope :precincts
                              :identifier :global
                              :error-type :missing-dependency}))))))

(deftest validate-v3-dependencies-test
  (let [validator (validate-dependencies v3-0-file-dependencies)]
    (testing "polling_location.txt dependencies"
      (testing "with precinct splits"
        (let [errors-chan (a/chan 100)
              ctx {:csv-source-file-paths
                   [(Paths/get "polling_location.txt" (into-array String []))
                    (Paths/get "precinct.txt" (into-array String []))
                    (Paths/get "precinct_split.txt" (into-array String []))
                    (Paths/get "polling_location.txt" (into-array String []))
                    (Paths/get "precinct_split_polling_location.txt" (into-array String []))]
                   :errors-chan errors-chan
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)
              errors (all-errors errors-chan)]
          (assert-no-problems errors {})))
      (testing "with precincts"
        (let [errors-chan (a/chan 100)
              ctx {:csv-source-file-paths
                   [(Paths/get "polling_location.txt" (into-array String []))
                    (Paths/get "precinct.txt" (into-array String []))
                    (Paths/get "polling_location.txt" (into-array String []))
                    (Paths/get "precinct_polling_location.txt" (into-array String []))]
                   :errors-chan errors-chan
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)
              errors (all-errors errors-chan)]
          (assert-no-problems errors {}))))))
