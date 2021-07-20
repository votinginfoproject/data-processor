(ns vip.data-processor.pipeline
  (:require [clojure.tools.logging :as log]
            [clojure.stacktrace :as stacktrace]
            [vip.data-processor.db.postgres :as psql]
            [clojure.core.async :as a]))

(defn try-processing-fn
  "Attempt to run the processing function on the context. If the
  processing function throws, add a `:stop` key and the `Throwable` to
  the context."
  [processing-fn ctx]
  (try
    (processing-fn ctx)
    (catch Throwable e
      (log/error e)
      (log/error ctx)
      (assoc ctx :stop "Exception caught"
                 :thrown-by processing-fn
                 :exception e))))

(defn check-stop-flag
  [ctx]
  (log/debug "Checking stop request flag")
  (if-let [stop-requested (psql/get-run-field ctx :stop_requested)]
    (do (log/info "db stop requested by" stop-requested)
        (assoc ctx :stop "Stop requested by" stop-requested))
    ctx))

(defn run-pipeline
  "Run the `pipeline` attached to a processing context. Will return
  the context after all processing functions in the pipeline have
  been completed or until a `:stop` key is added to the context."
  [c]
  (loop [ctx c]
    (let [[next-step & rest-pipeline] (:pipeline ctx)]
      (if next-step
        (let [ctx-with-rest-pipeline (assoc ctx :pipeline rest-pipeline)
              check-stop-flag-fn (:check-stop-flag-fn ctx)
              next-ctx (cond->> ctx-with-rest-pipeline
                         true (try-processing-fn next-step)
                         check-stop-flag-fn (check-stop-flag-fn))]
          (if (:stop next-ctx)
            next-ctx
            (recur next-ctx)))
        ctx))))

(defn process
  "A pipeline is a sequence of functions that take and return a
  `processing context`. The `initial-input` should be a map and will
  be merged with defaults before being passed as the processing
  context for the first function in the pipeline.

  Runs the pipeline, returning the final context. An exception on the
  context will result in logging the exception."
  ([pipeline initial-input]
   (process pipeline initial-input nil))
  ([pipeline initial-input delete-callback]
   (log/info "pipeline process kicking off with initial-input:"
             (pr-str initial-input))
   (let [ctx (merge {:spec-version nil
                     :spec-family nil
                     :errors-chan (a/chan 1024)
                     :pipeline pipeline
                     :check-stop-flag-fn check-stop-flag}
                    initial-input
                    (when-not (nil? delete-callback)
                      {:delete-callback delete-callback}))
         result (run-pipeline ctx)
         import-id (:import-id result)]
     (log/info (pr-str (select-keys result [:import-id :public-id :db :xml-output-file])))

     (when-let [stop (:stop result)]
       (psql/delete-run import-id)
       (log/error "Stopping run of" import-id "due to:" stop))

     (when-let [ex (:exception result)]
       (psql/fail-run import-id (with-out-str (stacktrace/print-throwable ex)))
       (log/error (with-out-str (stacktrace/print-stack-trace ex))))

     result)))
