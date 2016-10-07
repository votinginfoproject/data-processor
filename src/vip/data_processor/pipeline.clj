(ns vip.data-processor.pipeline
  (:require [clojure.tools.logging :as log]
            [clojure.stacktrace :as stacktrace]
            [vip.data-processor.db.postgres :as psql]
            [clojure.core.async :as a]
            [vip.data-processor.errors :as errors]))

(defn try-processing-fn
  "Attempt to run the processing function on the context. If the
  processing function throws, add a `:stop` key and the `Throwable` to
  the context."
  [processing-fn ctx]
  (try
    (processing-fn ctx)
    (catch Throwable e
      (log/error e)
      (assoc ctx :stop "Exception caught"
                 :thrown-by processing-fn
                 :exception e))))

(defn run-pipeline
  "Run the `pipeline` attached to a processing context. Will return
  the context after all processing functions in the pipeline have
  been completed or until a `:stop` key is added to the context."
  [c]
  (loop [ctx c]
    (let [[next-step & rest-pipeline] (:pipeline ctx)]
      (if next-step
        (let [ctx-with-rest-pipeline (assoc ctx :pipeline rest-pipeline)
              next-ctx (try-processing-fn next-step ctx-with-rest-pipeline)]
          (if (:stop next-ctx)
            next-ctx
            (recur next-ctx)))
        ctx))))

(defn process
  "A pipeline is a sequence of functions that take and return a
  `processing context`. The `initial-input` will be placed as the
  `:input` on the processing context for the first function in the
  pipeline.

  Runs the pipeline, returning the final context. An exception on the
  context will result in logging the exception."
  [pipeline initial-input]
  (let [spec-version (atom nil)
        errors-chan (a/chan 1024)
        ctx {:input initial-input
             :skip-validations? false
             :warnings {}
             :errors {}
             :critical {}
             :fatal {}
             :spec-version spec-version
             :errors-chan errors-chan
             :pipeline pipeline}
        _ (add-watch spec-version :route-errors
                     (errors/add-watch-fn errors-chan))
        result (run-pipeline ctx)
        import-id (:import-id result)]
    (log/info (pr-str (select-keys result [:import-id :public-id :db :xml-output-file])))

    (when-let [stop (:stop result)]
      (psql/fail-run import-id nil)
      (log/error "Stopping run of" import-id "due to:" stop))

    (when-let [ex (:exception result)]
      (psql/fail-run import-id (with-out-str (stacktrace/print-throwable ex)))
      (log/error (with-out-str (stacktrace/print-stack-trace ex))))

    result))
