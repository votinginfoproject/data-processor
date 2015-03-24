(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.db.duplicate-records :as dupe-records]
            [vip.data-processor.validation.db.duplicate-ids :as dupe-ids]
            [vip.data-processor.db.sqlite :as sqlite]))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (dupe-ids/duplicated-ids ctx)]
    (if-not (empty? dupes)
      (assoc-in ctx [:errors "Duplicate IDs"] dupes)
      ctx)))

(defn validate-no-duplicated-rows [csv-specs]
  (fn [ctx]
    (reduce dupe-records/validate-no-duplicated-rows-in-table ctx csv-specs)))

(defn validate-references-for-csv-spec [ctx {:keys [filename table columns]}]
  (let [reference-columns (filter :references columns)]
    (reduce (fn [ctx column]
              (let [unmatched-references (sqlite/unmatched-references
                                          (:tables ctx)
                                          table
                                          (:name column)
                                          (:references column))]
                (if (seq unmatched-references)
                  (assoc-in ctx [:errors
                                 filename
                                 :reference-error
                                 (:name column)]
                            unmatched-references)
                  ctx)))
            ctx
            reference-columns)))

(defn validate-references [csv-specs]
  (fn [ctx]
    (reduce validate-references-for-csv-spec ctx csv-specs)))

(defn validate-jurisdiction-reference [ctx {:keys [filename table]}]
  (let [unmatched-references (sqlite/unmatched-jurisdiction-references
                              (:tables ctx) table)]
    (if (seq unmatched-references)
      (assoc-in ctx [:errors filename :reference-error "jurisdiction_id"]
                unmatched-references)
      ctx)))

(defn validate-jurisdiction-references [csv-specs]
  (fn [ctx]
    (let [jurisdiction-tables (filter
                               (fn [spec] (some #{"jurisdiction_id"}
                                                (map :name (:columns spec))))
                               csv-specs)]
      (reduce validate-jurisdiction-reference ctx jurisdiction-tables))))
