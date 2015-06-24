(ns vip.data-processor.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [korma.core :as korma]))

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

(defn assert-column [ctx table column values]
  (is (= values
         (map column
              (korma/select (get-in ctx [:tables table])
                            (korma/fields column)
                            (korma/order :id :ASC))))))

(defn flatten-keys* [a ks m]
  (if (map? m)
    (reduce into (map (fn [[k v]] (flatten-keys* a (conj ks k) v)) (seq m)))
    (assoc a ks m)))

(defn flatten-keys [m] (flatten-keys* {} [] m))

(defn assert-error-format
  [out-ctx]
  (doseq [severity problem-types]
    (if-let [errors (get out-ctx severity)]
      (let [flattened (flatten-keys errors)]
        (doseq [[path-to-errors errors] flattened]
          (is (keyword? (first path-to-errors)))
          (is (= 3 (count path-to-errors)))
          (is (sequential? errors))
          (is (not (empty? errors))))))))
