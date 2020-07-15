(ns vip.data-processor.db.translations.v5-2.polling-locations
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-2-tables :polling-locations)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.PollingLocation." index))

(defn transform-fn
  [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [(util/simple-value->ltree :address_line)
              (util/internationalized-text->ltree :directions)
              (util/internationalized-text->ltree :hours)
              (util/simple-value->ltree :hours_open_id)
              (util/simple-value->ltree :is_drop_box)
              (util/simple-value->ltree :is_early_voting)
              util/latlng->ltree
              (util/simple-value->ltree :name)
              (util/simple-value->ltree :photo_uri)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer
 (util/transformer row-fn transform-fn))
