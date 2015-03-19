(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]))

(defn has-id? [csv-spec]
  (some #(-> % :name (= "id")) (:columns csv-spec)))

(defn tables-with-ids [ctx]
  (let [csvs-with-id (filter has-id? csv/csv-specs)
        table-names (map :table csvs-with-id)]
    (map (:tables ctx) table-names)))

(defn add-to-seen-ids [seen-ids table]
  (let [ids (korma/select table (korma/fields :id))]
    (reduce (fn [seen-ids {:keys [id]}]
              (update seen-ids id conj (:table table)))
            seen-ids
            ids)))

(defn duplicated-ids [ctx]
  (let [tables (tables-with-ids ctx)
        all-ids (reduce add-to-seen-ids {} tables)]
    (apply dissoc all-ids (for [[k v] all-ids :when (= 1 (count v))] k))))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (duplicated-ids ctx)]
    (if-not (empty? dupes)
      (assoc-in ctx [:errors "Duplicate IDs"] dupes)
      ctx)))
