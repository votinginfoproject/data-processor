(ns vip.data-processor.validation.csv.file-set-test
  (:require [vip.data-processor.validation.csv.file-set :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [clojure.test :refer :all]
            [clojure.core.async :as a])
  (:import [java.io File]))

(deftest validate-dependencies-test
  (let [dependencies (build-dependencies
                      "ballot.txt" (and "candidate.txt"
                               (or "precinct.txt" "election.txt"))
                      "precinct.txt" "source.txt")
        validator (validate-dependencies dependencies)]
    (testing "does not add errors when dependencies are met"
      (let [ctx {:input [(File. "ballot.txt")
                         (File. "candidate.txt")
                         (File. "election.txt")]
                 :data-specs v3-0/data-specs}
            out-ctx (validator ctx)]
        (assert-no-problems out-ctx []))
      (testing "with sub-dependencies"
        (let [ctx {:input [(File. "ballot.txt")
                           (File. "candidate.txt")
                           (File. "precinct.txt")
                           (File. "source.txt")]
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)]
          (assert-no-problems out-ctx []))))
    (testing "adds errors when dependencies are not met"
      (let [errors-chan (a/chan 100)
            ctx {:input [(File. "ballot.txt")
                         (File. "precinct.txt")]
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
              ctx {:input [(File. "polling_location.txt")
                           (File. "precinct.txt")
                           (File. "precinct_split.txt")
                           (File. "polling_location.txt")
                           (File. "precinct_split_polling_location.txt")]
                   :errors-chan errors-chan
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)
              errors (all-errors errors-chan)]
          (assert-no-problems-2 errors {})))
      (testing "with precincts"
        (let [errors-chan (a/chan 100)
              ctx {:input [(File. "polling_location.txt")
                           (File. "precinct.txt")
                           (File. "polling_location.txt")
                           (File. "precinct_polling_location.txt")]
                   :errors-chan errors-chan
                   :data-specs v3-0/data-specs}
              out-ctx (validator ctx)
              errors (all-errors errors-chan)]
          (assert-no-problems-2 errors {}))))))
