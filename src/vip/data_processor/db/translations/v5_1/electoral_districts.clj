(ns vip.data-processor.db.translations.v5-1.electoral-districts
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn electoral-districts [import-id]
  (korma/select (postgres/v5-1-tables :electoral-districts)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.ElectoralDistrict." index))

(defn electoral-district->ltree-entries [idx-fn electoral-district]
  (let [electoral-district-path (base-path (idx-fn))
        id-path (util/id-path electoral-district-path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn electoral-district-path electoral-district)
             [util/external-identifiers->ltree
              (util/simple-value->ltree :name)
              (util/simple-value->ltree :number)
              (util/simple-value->ltree :type)
              (util/simple-value->ltree :other_type)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id electoral-district)})))

(def transformer (util/transformer electoral-districts
                                   electoral-district->ltree-entries))
