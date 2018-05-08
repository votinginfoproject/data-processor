(ns vip.data-processor.errors
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(defrecord ValidationError [ctx severity scope
                            identifier error-type error-value])

(defn add-errors
  "Adds an error to the validations.
  severity: keyword, one of :fatal, :critical, :errors, :warnings
  scope: keyword, roughly the location of the error (:precinct, :id, :import, :global)
  identifier: If it's a specific type like Precinct, the ID of the Precinct
  error-type: keyword, specific subclass of error (:duplicate, :missing-data)
  error-data: one or more value that caused the error (ie bad zip code)."
  [{:keys [errors-chan] :as ctx}
   severity scope identifier error-type
   & error-data]
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->ValidationError ctx severity scope identifier error-type error-value)))
  (if (= severity :fatal)
    (assoc ctx :fatal-errors? true)
    ctx))

(defrecord V5ValidationError [ctx severity scope
                                identifier error-type error-value parent-element-id])

(defn add-v5-errors
  "Same error options as add-errors, also with a parent-element-id which
   is often used to associate some child error on, say, Id, to the element it
   is in, ie Precinct. This is important since we drop the table that was
   previously used to look this up. So if there is a parent element associated
   to the particular error type, it's important to use this instead of
   `add-errors` to preserve that information. If your error is of a more
   global type of nature, you can just use `add-errors`."
  [{:keys [errors-chan] :as ctx}
   severity scope identifier error-type parent-element-id
   & error-data]
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->V5ValidationError ctx severity scope identifier error-type error-value parent-element-id)))
  (if (= severity :fatal)
    (assoc ctx :fatal-errors? true)
    ctx))

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
