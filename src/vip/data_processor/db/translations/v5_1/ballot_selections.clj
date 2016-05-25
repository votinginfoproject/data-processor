(ns vip.data-processor.db.translations.v5-1.ballot-selections
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn ballot-selections [import-id]
  (korma/select (postgres/v5-1-tables :ballot-selections)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.BallotSelection." index))

(defn ballot-selection->ltree-entries [idx-fn ballot-selection]
  (let [ballot-selection-path (base-path (idx-fn))
        id-path (util/id-path ballot-selection-path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn ballot-selection-path ballot-selection)
             [(util/simple-value->ltree :sequence_order)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id ballot-selection)})))

(def transformer (util/transformer ballot-selections
                                   ballot-selection->ltree-entries))
