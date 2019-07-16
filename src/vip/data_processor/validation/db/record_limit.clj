(ns vip.data-processor.validation.db.record-limit
  (:require [korma.core :as korma]
            [vip.data-processor.errors :as errors]))

(defn count-rows [table]
  "Takes a single table and counts the number of rows in it."
  (:cnt (first (korma/select table (korma/aggregate (count "*") :cnt)))))

(defn name-and-count [tables]
  "Transforms the selected tables into a map of names and counts."
  (map
   (fn [[table-name table]]
     {:name table-name :count (count-rows table)})
   tables))

(defn error-if-not-one-row [ctx count]
  "If the count of the rows is not equal to 1, add an error."
  (if-not (= 1 (:count count))
    (do
      (println (:name count) " has " (:count count) " rows!")
      (errors/add-errors ctx :errors (:name count) :global :row-constraint
                           "File needs to contain exactly one row."))
    ctx))

(defn tables-allow-only-one-record
  "Sort through the ctx to select out specific tables as defined in
  the data spec. Then, count the rows and return errors for any that
  do not have exactly one row."
  [{:keys [data-specs tables] :as ctx}]
  (let [tables (->> data-specs
                    (filter :single-row)
                    (map :table)
                    (select-keys tables))
        counts (name-and-count tables)]
    (reduce error-if-not-one-row ctx counts)))
