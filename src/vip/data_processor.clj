(ns vip.data-processor
  (:require [clojure.edn :as edn]
            [democracyworks.squishy :as sqs]
            [vip.data-processor.s3 :as s3])
  (:gen-class))

(defn process-file [message]
  (let [{:keys [filename]} (edn/read-string (:body message))
        file (s3/download filename)]
    (println (pr-str file))))

(defn consume []
  (sqs/consume-messages (sqs/client) process-file))

(defn -main [& args]
  (println "VIP Data Processor starting up.")
  (let [consumer (consume)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (println "VIP Data Processor shutting down...")
                                 (future-cancel consumer))))
    (while true)))
