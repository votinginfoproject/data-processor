(ns vip.data-processor.pipeline
  (:require [clojure.tools.logging :as log]
            [clojure.stacktrace :as stacktrace]))

(defn try-validation [validation ctx]
  (try
    (validation ctx)
    (catch Throwable e
      (log/error e)
      (assoc ctx :stop "Exception caught"
                 :exception e))))

(defn run-pipeline [c]
  (loop [ctx c]
    (if-let [next-step (-> ctx :pipeline first)]
      (let [next-ctx (try-validation next-step ctx)]
        (if (:stop next-ctx)
          next-ctx
          (recur (assoc next-ctx :pipeline (rest (:pipeline ctx))))))
      ctx)))

(defn process [pipeline initial-input]
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
