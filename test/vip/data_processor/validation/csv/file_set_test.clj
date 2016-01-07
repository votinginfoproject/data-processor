(ns vip.data-processor.validation.csv.file-set-test
  (:require [vip.data-processor.validation.csv.file-set :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [clojure.test :refer :all])
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
      (let [ctx {:input [(File. "ballot.txt")
                         (File. "precinct.txt")]
                 :data-specs v3-0/data-specs}
            out-ctx (validator ctx)]
        (assert-some-problem out-ctx [:ballots :global :missing-dependency])
        (assert-some-problem out-ctx [:precincts :global :missing-dependency])
        (assert-error-format out-ctx)))))
