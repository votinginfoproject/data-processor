(ns vip.data-processor.pipeline
  (:require [clojure.tools.logging :as log]))

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

;;; TODO: throw exception (if there is one) so that squishy will put
;;; the message that started this whole process on the fail queue
(defn process [pipeline initial-input]
  (let [ctx {:input initial-input
             :pipeline pipeline}
        result (run-pipeline ctx)]
    (log/info result)
    result))
