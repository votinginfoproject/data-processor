(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.db.duplicate-records :as dupe-records]
            [vip.data-processor.validation.db.duplicate-ids :as dupe-ids]
            [vip.data-processor.validation.db.references :as refs]
            [vip.data-processor.validation.db.reverse-references :as rev-refs]
            [vip.data-processor.validation.db.street-segment :as street-segment]))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (dupe-ids/duplicated-ids ctx)]
    (if-not (empty? dupes)
      (assoc-in ctx [:errors "Duplicate IDs"] dupes)
      ctx)))

(defn validate-no-duplicated-rows [{:keys [csv-specs] :as ctx}]
  (reduce dupe-records/validate-no-duplicated-rows-in-table ctx (:csv-specs ctx)))

(defn validate-references [{:keys [csv-specs] :as ctx}]
  (reduce refs/validate-references-for-csv-spec ctx csv-specs))

(defn validate-jurisdiction-references [{:keys [csv-specs] :as ctx}]
  (let [jurisdiction-tables (filter
                               (fn [spec] (some #{"jurisdiction_id"}
                                                (map :name (:columns spec))))
                               csv-specs)]
    (reduce refs/validate-jurisdiction-reference ctx jurisdiction-tables)))

(defn validate-no-unreferenced-rows [{:keys [csv-specs] :as ctx}]
  (let [referenced-tables (rev-refs/find-all-referenced-tables csv-specs)]
    (reduce rev-refs/validate-no-unreferenced-rows-for-table ctx referenced-tables)))

(defn validate-no-overlapping-street-segments [ctx]
  (let [street-segments (get-in ctx [:tables :street-segments])
        overlaps (street-segment/query-overlaps street-segments)]
    (if (seq overlaps)
      (assoc-in ctx [:errors "street_segment.txt" :overlaps] overlaps)
      ctx)))
