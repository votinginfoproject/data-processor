(ns vip.data-processor.db.translations.v5-2.street-segments
  (:require [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(def row-fn
  (postgres/lazy-cursor-fetch
   10000
   (fn [import-id]
     (str "SELECT * FROM v5_1_street_segments"
          " WHERE results_id = " import-id))))

(defn base-path [index]
  (str "VipObject.0.StreetSegment." index))

(defn transform-fn [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [(util/simple-value->ltree :address_direction)
              (util/simple-value->ltree :city)
              (util/simple-value->ltree :includes_all_addresses)
              (util/simple-value->ltree :includes_all_streets)
              (util/simple-value->ltree :odd_even_both)
              (util/simple-value->ltree :precinct_id)
              (util/simple-value->ltree :start_house_number)
              (util/simple-value->ltree :end_house_number)
              (util/simple-value->ltree :state)
              (util/simple-value->ltree :street_direction)
              (util/simple-value->ltree :street_name)
              (util/simple-value->ltree :street_suffix)
              (util/simple-value->ltree :unit_number)
              (util/simple-value->ltree :zip)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer (util/transformer-with-conn row-fn transform-fn))
