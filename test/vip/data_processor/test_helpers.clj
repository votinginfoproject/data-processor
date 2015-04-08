(ns vip.data-processor.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]))

(set! *print-length* 10)

(def problem-types [:warnings :errors :critical :fatal])

(defn assert-no-problems
  "Test that there are no errors of any level for the key-path below
  that level.

    (assert-no-problems ctx [:missing :ballot])

  That will check there is nothing
  at [:warnings :missing :ballot], [:errors :missing :ballot], etc."
  [ctx key-path]
  (doseq [problem-type problem-types]
    (is (empty? (get-in ctx (cons problem-type key-path))))))

(defn assert-some-problem
  "Test that there is an error of some level for the key-path."
  [ctx key-path]
  (is (seq (remove nil? (map #(get-in ctx (cons % key-path)) problem-types)))))

(defn csv-inputs [file-names]
  (map #(->> %
             (str "csv/")
             io/resource
             io/as-file)
       file-names))

(defn xml-input [file-name]
  [(->> file-name
         (str "xml/")
         io/resource
         io/as-file)])
