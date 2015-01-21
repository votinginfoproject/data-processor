(ns vip.data-processor
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [democracyworks.squishy :as sqs]
            [vip.data-processor.s3 :as s3])
  (:gen-class))

(defn process-file [message]
  (let [{:keys [filename]} (edn/read-string (:body message))
        file (s3/download filename)]
    (log/info "Downloaded" (pr-str file))))

(defn consume []
  (sqs/consume-messages (sqs/client) process-file))

(defn -main [& args]
  (log/info "VIP Data Processor starting up.")
  (let [consumer (consume)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (log/info "VIP Data Processor shutting down...")
                                 (future-cancel consumer))))
    (while true)))
