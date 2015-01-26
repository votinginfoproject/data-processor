(ns vip.data-processor.pipeline
  (:require [clojure.tools.logging :as log]
            [clojure.stacktrace :as stacktrace]))

(defn try-validation [validation ctx]
  (try
    (validation ctx)
    (catch Throwable e
      (log/error e)
      (assoc ctx :stop "Exception caught"
                 :thrown-by validation
                 :exception e))))

(defn run-pipeline [c]
  (loop [ctx c]
    (let [pipeline (:pipeline ctx)]
      (if-let [next-step (first pipeline)]
        (let [ctx-with-rest-pipeline (assoc ctx :pipeline (rest pipeline))
              next-ctx (try-validation next-step ctx-with-rest-pipeline)]
          (if (:stop next-ctx)
            next-ctx
            (recur next-ctx)))
        ctx))))

(defn process
  "A pipeline is a sequence of functions that take and return a
  `processing context`. The `initial-input` will be placed as the
  `:input` on the processing context for the first function in the
  pipeline."
  [pipeline initial-input]
  (let [ctx {:input initial-input
             :warnings {}
             :errors {}
             :pipeline pipeline}
        result (run-pipeline ctx)]
    (log/info result)
    (when-let [ex (:exception result)]
      (log/error (with-out-str (stacktrace/print-stack-trace ex)))
      (throw (ex-info "Exception during processing" {:exception ex
                                                     :initial-ctx ctx
                                                     :final-ctx result})))
    result))
