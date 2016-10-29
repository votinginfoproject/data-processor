(ns vip.data-processor.errors
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(defrecord ValidationError [ctx severity scope
                            identifier error-type error-value])

(defn add-errors [{:keys [errors-chan] :as ctx}
                  severity scope identifier error-type
                  & error-data]
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->ValidationError ctx severity scope identifier error-type error-value)))

  ctx)

(defn close-errors-chan [{:keys [errors-chan] :as ctx}]
  (a/close! errors-chan)
  ctx)

(defn await-statistics
  "The process validations functions use batch-process from
  utility-fns.async and puts the resulting core.async channel on the
  context at :processing-chan. Once the last validations have been
  saved, the feed's statistics will be calculated on the thread that
  coordinates the work. Thus, here we wait on the core.async channel
  and we'll be sure the stats are done."
  [ctx]
  (log/info "Awaiting statistics")
  (-> ctx
      :processing-chan
      a/<!!)
  (log/info "Statistics complete")
  ctx)
