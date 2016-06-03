(ns vip.data-processor.db.translations.v5-1.localities
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]
            [clojure.string :as str]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-1-tables :localities)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.Locality." index))

(defn transform-fn [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [(util/simple-value->ltree :election_administration_id)
              util/external-identifiers->ltree
              (util/simple-value->ltree :name)
              (util/simple-value->ltree :polling_location_ids)
              (util/simple-value->ltree :state_id)
              (util/simple-value->ltree :type)
              (util/simple-value->ltree :other_type)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer (util/transformer row-fn transform-fn))
