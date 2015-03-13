(ns vip.data-processor.validation.csv.file-set-test
  (:require [vip.data-processor.validation.csv.file-set :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.test :refer :all])
  (:import [java.io File]))

(deftest validate-dependencies-test
  (let [dependencies (build-dependencies
                      "a" (and "b"
                               (or "c" "d"))
                      "c" "e")
        validator (validate-dependencies dependencies)]
    (testing "does not add errors when dependencies are met"
      (let [ctx {:input [(File. "a")
                         (File. "b")
                         (File. "d")]}
            out-ctx (validator ctx)]
        (assert-no-problems out-ctx [:file-dependencies]))
      (testing "with sub-dependencies"
        (let [ctx {:input [(File. "a")
                           (File. "b")
                           (File. "c")
                           (File. "e")]}
              out-ctx (validator ctx)]
          (assert-no-problems out-ctx [:file-dependencies]))))
    (testing "adds errors when dependencies are not met"
      (let [ctx {:input [(File. "a")
                         (File. "c")]}
            out-ctx (validator ctx)]
        (assert-some-problem out-ctx [:file-dependencies "a"])
        (assert-some-problem out-ctx [:file-dependencies "c"])))))
