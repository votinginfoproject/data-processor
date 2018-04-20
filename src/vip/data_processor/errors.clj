(ns vip.data-processor.errors
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(defrecord ValidationError
  [ctx severity scope identifier error-type error-value])

(defn add-errors
  "Adds an error to the validations.
  severity: keyword, one of :fatal, :critical, :errors, :warnings
  scope: keyword, roughly the location of the error (:precinct, :id, :import, :global)
  identifier: If it's a specific type like Precinct, the ID of the Precinct
  error-type: keyword, specific subclass of error (:duplicate, :missing-data)
  error-data: one or more value that caused the error (ie bad zip code).

  NOTE: if you call this function when processing a v5 feed, then keep
  in mind that, *unless your identifier is `:global` or
  `:post-process-street-segments`*, then at the time the validation
  (error is stored in the database, it will attempt to look up a path
  for the identifier value. A bad or nonexistent identifier can break
  statistic generation so this is important to be aware of."
  [{:keys [errors-chan] :as ctx}
   severity scope identifier error-type
   & error-data]
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->ValidationError ctx severity scope identifier error-type error-value)))
  ctx)

(defn record-error!
  "Wrapper around `add-errors` to pass error arguments as a map so
  argument types are explicit at the call site and so argument order
  is irrelevant. See `add-errors` for a description of field values."
  [ctx {:keys [severity scope identifier error-type error-data]}]
  (if (seq error-data)
    ;; we apply because the last arg, error-data, is varargs
    (apply
     add-errors
     ctx severity scope identifier error-type error-data)
    (add-errors ctx severity scope identifier error-type)))

(defrecord V5ValidationError
  [ctx severity scope identifier error-type error-value parent-element-id])

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
