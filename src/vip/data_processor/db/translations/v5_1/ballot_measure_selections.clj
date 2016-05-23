(ns vip.data-processor.db.translations.v5-1.ballot-measure-selections
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn ballot-measure-selections [import-id]
  (korma/select (postgres/v5-1-tables :ballot-measure-selections)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.BallotMeasureSelection." index))

(defn bms->ltree-entries [idx-fn bms]
  (let [bms-path (base-path (idx-fn))
        id-path (util/id-path bms-path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn bms-path bms)
             [(util/simple-value->ltree :sequence_order)
              (util/internationalized-text->ltree :selection)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id bms)})))

(def transformer (util/transformer ballot-measure-selections
                                   bms->ltree-entries))
