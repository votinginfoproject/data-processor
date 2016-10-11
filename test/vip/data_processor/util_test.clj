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

(deftest flatten-keys-test
  (testing "a nested map collapses into a single map"
    (is (= {[:some :path :to :a] "value"}
           (flatten-keys {:some {:path {:to {:a "value"}}}}))))

  (testing "when we run into a vector, that becomes part of the value"
    (is (= {[:to :flatten :or] [:not {:to "flatten"}]}
           (flatten-keys {:to {:flatten {:or [:not {:to "flatten"}]}}})))))

(deftest version-without-patch-test
  (testing "when there is no version, there is no patch level to remove"
    (is (nil? (version-without-patch nil)))
    (is (nil? (version-without-patch ""))))

  (testing "with a two-part version number, we get our input back"
    (is (= "5.1" (version-without-patch "5.1"))))

  (testing "with a three-part version number, we strip the patch level"
    (is (= "5.1" (version-without-patch "5.1.1")))))
