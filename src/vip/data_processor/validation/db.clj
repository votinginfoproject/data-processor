(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.db.duplicate-records :as dupe-records]
            [vip.data-processor.validation.db.duplicate-ids :as dupe-ids]
            [vip.data-processor.validation.db.references :as refs]
            [vip.data-processor.validation.db.record-limit :as record-limit]
            [vip.data-processor.validation.db.reverse-references :as rev-refs]
            [vip.data-processor.validation.db.street-segment :as street-segment]
            [vip.data-processor.validation.db.admin-addresses :as admin-addresses]
            [vip.data-processor.validation.fips :as fips]))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (dupe-ids/duplicated-ids ctx)]
    (reduce (fn [ctx [id tables]]
              (assoc-in ctx [:errors :import id :duplicate-ids] tables))
            ctx dupes)))

(defn validate-no-duplicated-rows [{:keys [data-specs] :as ctx}]
  (reduce dupe-records/validate-no-duplicated-rows-in-table ctx data-specs))

(defn validate-one-record-limit [ctx]
  (record-limit/tables-allow-only-one-record ctx))

(defn validate-references [{:keys [data-specs] :as ctx}]
  (reduce refs/validate-references-for-data-spec ctx data-specs))

(defn validate-jurisdiction-references [{:keys [data-specs] :as ctx}]
  (let [jurisdiction-tables (filter
                               (fn [spec] (some #{"jurisdiction_id"}
                                                (map :name (:columns spec))))
                               data-specs)]
    (reduce refs/validate-jurisdiction-reference ctx jurisdiction-tables)))

(defn validate-no-unreferenced-rows [{:keys [data-specs] :as ctx}]
  (let [referenced-tables (rev-refs/find-all-referenced-tables data-specs)]
    (reduce rev-refs/validate-no-unreferenced-rows-for-table ctx referenced-tables)))

(defn validate-no-overlapping-street-segments [ctx]
  (let [street-segments (get-in ctx [:tables :street-segments])
        overlaps (->> street-segments
                      street-segment/query-overlaps
                      (map vals)
                      (map set)
                      set)]
    (reduce (fn [ctx overlap]
              (let [[id overlap-id] (sort overlap)]
                (update-in ctx [:errors :street-segments id :overlaps]
                           conj overlap-id)))
            ctx overlaps)))

(defn validate-election-administration-addresses [ctx]
  (admin-addresses/validate-addresses ctx))

(def validations
  [validate-no-duplicated-ids
   validate-no-duplicated-rows
   validate-references
   validate-jurisdiction-references
   validate-one-record-limit
   validate-no-unreferenced-rows
   validate-no-overlapping-street-segments
   validate-election-administration-addresses
   fips/validate-valid-source-vip-id])
