(ns vip.data-processor.validation.db.duplicate-ids
  (:require [korma.core :as korma]))

(defn has-id? [data-spec]
  (some #(-> % :name (= "id")) (:columns data-spec)))

(defn tables-with-ids [{:keys [data-specs tables]}]
  (->> data-specs
       (filter has-id?)
       (map :table)
       (map tables)))

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
