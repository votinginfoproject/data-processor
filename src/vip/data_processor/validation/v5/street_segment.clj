(ns vip.data-processor.validation.v5.street-segment
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.string :as str]
            [vip.data-processor.errors :as errors]
            [clojure.tools.logging :as log]))

(util/validate-no-missing-values :street-segment
                                 [:odd-even-both]
                                 [:city]
                                 [:state]
                                 [:street-name]
                                 [:zip])

(def validate-odd-even-both-value
  (util/validate-enum-elements :oeb-enum :errors))

(defn validate-no-street-segment-overlaps
  [{:keys [import-id] :as ctx}]
  (log/info "Validating street segment overlaps")
  (let [overlaps (korma/exec-raw
                  (:conn postgres/v5-1-street-segments)
                  ["SELECT * from street_segment_overlaps(?);" [import-id]]
                  :results)]
    (reduce (fn [ctx overlap]
              (let [parent-element-id (->(korma/exec-raw
                                          (:conn postgres/xml-tree-values)
                                          ["SELECT value
                                            FROM xml_tree_values
                                            WHERE path = subpath(text2ltree(?),0,4) || 'id'
                                            and results_id = ?" [(-> :path overlap .getValue)  import-id]]
                                          :results)
                                        first
                                        :value)]
                (errors/v5-add-errors ctx
                                   :errors
                                   :street-segment
                                   (.getValue (:path overlap))
                                   :overlaps
                                   parent-element-id
                                   (:id overlap))))
            ctx overlaps)))
