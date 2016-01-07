(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.db.duplicate-records :as dupe-records]
            [vip.data-processor.validation.db.duplicate-ids :as dupe-ids]
            [vip.data-processor.validation.db.references :as refs]
            [vip.data-processor.validation.db.record-limit :as record-limit]
            [vip.data-processor.validation.db.reverse-references :as rev-refs]
            [com.climate.newrelic.trace :refer [defn-traced]]))

(defn-traced validate-no-duplicated-ids [ctx]
  (let [dupes (dupe-ids/duplicated-ids ctx)]
    (reduce (fn [ctx [id tables]]
              (assoc-in ctx [:errors :import id :duplicate-ids] tables))
            ctx dupes)))

(defn-traced validate-no-duplicated-rows [{:keys [data-specs] :as ctx}]
  (let [tables-to-check (remove :ignore-duplicate-records data-specs)]
    (reduce dupe-records/validate-no-duplicated-rows-in-table ctx tables-to-check)))

(defn-traced validate-one-record-limit [ctx]
  (record-limit/tables-allow-only-one-record ctx))

(defn-traced validate-references [{:keys [data-specs] :as ctx}]
  (reduce refs/validate-references-for-data-spec ctx data-specs))

(defn-traced validate-no-unreferenced-rows [{:keys [data-specs] :as ctx}]
  (let [referenced-tables (rev-refs/find-all-referenced-tables data-specs)]
    (reduce rev-refs/validate-no-unreferenced-rows-for-table ctx referenced-tables)))

(def validations
  [validate-no-duplicated-ids
   validate-no-duplicated-rows
   validate-references
   validate-one-record-limit
   validate-no-unreferenced-rows])
