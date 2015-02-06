(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [democracyworks.squishy :as sqs]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.queue :as q])
  (:gen-class))

(def pipeline
  [t/read-edn-sqs-message
   t/assert-filename
   t/download-from-s3
   zip/unzip
   zip/extracted-contents
   t/xml-csv-branch])

(defn consume []
  (sqs/consume-messages (sqs/client)
                        (fn [message]
                          (q/publish {:initial-input message
                                      :status :started}
                                     "processing.started")
                          (pipeline/process pipeline message)
                          (q/publish {:initial-input message
                                      :status :complete}
                                     "processing.complete"))))

(defn -main [& args]
  (let [id (java.util.UUID/randomUUID)]
    (log/info "VIP Data Processor starting up. ID:" id)
    (q/initialize)
    (q/publish {:id id :event "starting"} "qa-engine.status")
    (let [consumer (consume)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (log/info "VIP Data Processor shutting down...")
                                   (q/publish {:id id :event "stopping"} "qa-engine.status")
                                   (future-cancel consumer))))
      (while true))))
