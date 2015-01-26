(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [democracyworks.squishy :as sqs]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip])
  (:gen-class))

(def pipeline
  [t/read-edn-sqs-message
   t/assert-filename
   t/download-from-s3
   zip/unzip
   zip/extracted-contents
   t/xml-csv-branch])

(defn consume []
  (sqs/consume-messages (sqs/client) (partial pipeline/process pipeline)))

(defn -main [& args]
  (log/info "VIP Data Processor starting up.")
  (let [consumer (consume)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (log/info "VIP Data Processor shutting down...")
                                 (future-cancel consumer))))
    (while true)))
