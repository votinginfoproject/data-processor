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

(deftest format-date-test
  (testing "date-strings with forward-slashes are normalized"
    (is (= "1983-01-16" (format-date "1983/01/16"))))

  (testing "date-strings without a forward-slash pass through"
    (is (= "19830116" (format-date "19830116"))))

  (testing "but if it's a partial date, no dice!"
    (is (nil? (format-date "1983/01")))))

