(ns vip.data-processor.util-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.util :refer :all])
  (:import [java.io File]))

(deftest find-input-file-test
  (let [election-file (File. "/data/election.txt")
        state-file    (File. "/data/state.txt")
        upper-case-source (File. "/data/Source.txt")
        ctx {:input [election-file state-file upper-case-source]}]
    (testing "finds files from their name"
      (is (= election-file (find-input-file ctx "election.txt")))
      (is (= state-file    (find-input-file ctx "state.txt"))))
    (testing "returns nil if not found"
      (is (nil? (find-input-file ctx "DOES_NOT_EXIST.txt"))))
    (testing "finds files without regard to the file's case"
      (is (= upper-case-source (find-input-file ctx "source.txt"))))))
