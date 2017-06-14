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

(defn valid-house-number-or-nil?
  [house-number]
  (or (str/blank? house-number)
      (try
        (Integer/parseInt house-number)
        true
        (catch NumberFormatException ex
          false))))

(def validate-start-house-number
  (util/validate-elements :street-segment
                          [:start-house-number]
                          valid-house-number-or-nil?
                          :fatal
                          :start-house-number))

(def validate-end-house-number
  (util/validate-elements :street-segment
                          [:end-house-number]
                          valid-house-number-or-nil?
                          :fatal
                          :end-house-number))

(defn validate-no-street-segment-overlaps
  [{:keys [import-id] :as ctx}]
  (log/info "Validating street segment overlaps")
  (let [overlaps (korma/exec-raw
                  (:conn postgres/v5-1-street-segments)
                  ["SELECT * from street_segment_overlaps(?);" [import-id]]
                  :results)]
    (reduce (fn [ctx overlap]
              (let [path (-> :path overlap .getValue)
                    parent-element-id (util/get-parent-element-id path import-id)]
                (errors/add-v5-errors ctx
                                   :errors
                                   :street-segment
                                   path
                                   :overlaps
                                   parent-element-id
                                   (:id overlap))))
            ctx overlaps)))
