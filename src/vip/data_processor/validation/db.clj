(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.db.duplicate-records :as dupe-records]
            [vip.data-processor.validation.db.duplicate-ids :as dupe-ids]
            [vip.data-processor.validation.db.references :as refs]))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (dupe-ids/duplicated-ids ctx)]
    (if-not (empty? dupes)
      (assoc-in ctx [:errors "Duplicate IDs"] dupes)
      ctx)))

(defn validate-no-duplicated-rows [csv-specs]
  (fn [ctx]
    (reduce dupe-records/validate-no-duplicated-rows-in-table ctx csv-specs)))

(defn validate-references [csv-specs]
  (fn [ctx]
    (reduce refs/validate-references-for-csv-spec ctx csv-specs)))

(defn validate-jurisdiction-references [csv-specs]
  (let [jurisdiction-tables (filter
                               (fn [spec] (some #{"jurisdiction_id"}
                                                (map :name (:columns spec))))
                               csv-specs)]
    (fn [ctx]
      (reduce refs/validate-jurisdiction-reference ctx jurisdiction-tables))))
