(ns vip.data-processor.errors
  (:require [clojure.core.async :as a]))

(def v3-errors-chan (a/chan 1024))
(def v3-errors-mix (a/mix v3-errors-chan))

(def v5-errors-chan (a/chan 1024))
(def v5-errors-mix (a/mix v5-errors-chan))

(defn route-messages [errors-chan spec-version]
  (case spec-version
    "3.0" (a/admix v3-errors-mix errors-chan)
    "5.1" (a/admix v5-errors-mix errors-chan)))

(defn add-watch-fn [errors-chan]
  (fn [key ref _ spec-version]
    (route-messages errors-chan spec-version)
    (remove-watch ref key)))

(defrecord ValidationError [ctx severity scope
                            identifier error-type error-value])

(defn add-errors [{:keys [errors-chan] :as ctx}
                  severity scope identifier error-type
                  & error-data]
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->ValidationError ctx severity scope identifier error-type error-value)))

  ctx)

(defn detach-errors-chan [{:keys [errors-chan spec-version] :as ctx}]
  (case @spec-version
    "3.0" (a/unmix v3-errors-mix errors-chan)
    "5.1" (a/unmix v5-errors-mix errors-chan))
  ctx)
