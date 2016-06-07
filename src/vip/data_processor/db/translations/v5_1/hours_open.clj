(ns vip.data-processor.db.translations.v5-1.hours-open
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-1.schedules :as schedules]))

(defn row-fn [import-id]
  (let [schedules (korma/select (postgres/v5-1-tables :schedules)
                                (korma/where {:results_id import-id}))]
    (group-by :hours_open_id schedules)))

(defn base-path [index]
  (str "VipObject.0.HoursOpen." index))

(defn transform-fn [idx-fn [hours_open_id schedules]]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat (partial schedules/schedule->ltree path child-idx-fn)
             schedules)
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value hours_open_id})))

(def transformer (util/transformer row-fn transform-fn))
