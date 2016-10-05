(ns vip.data-processor.errors
  (:require [clojure.core.async :as a]))

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

(defn await-statistics [ctx]
  (a/<!! (:processing-chan ctx))
  ctx)
