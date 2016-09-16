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
  ;;; Async version:
  ;; (a/onto-chan errors-chan
  ;;            (map (partial ->ValidationError ctx severity scope identifier error-type)
  ;;                 error-data)
  ;;            false)

  ;;; Sync version:
  (doseq [error-value error-data]
    (a/>!! errors-chan
           (->ValidationError ctx severity scope identifier error-type error-value)))

  ctx)

(defn detach-errors-chan [{:keys [errors-chan spec-version] :as ctx}]
  (case @spec-version
    "3.0" (a/unmix v3-errors-mix errors-chan)
    "5.1" (a/unmix v5-errors-mix errors-chan))
  ctx)

;;; TODO:
;;; * DONE: Go back to one add-errors-fn
;;; * DONE: make spec-version an atom, add a watcher from `add-watch-fn`
;;; * DONE: update usages of spec-version deref the atom
;;; * DONE: make new `batch-process` calls for each v*errors-chan (both
;;;   data_processor.clj and dev-src/core.clj)
;;; * DONE: try it out
;;; * DONE: make the batch processors bulk-insert into tables
;;; * update tests
;;; * update batch-size and timeout for the batch processors
;;; * make it blocking? (UNDO make it blocking?)
