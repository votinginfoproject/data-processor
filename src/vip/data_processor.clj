(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [democracyworks.squishy :as sqs]
            [korma.core :as korma]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.queue :as q]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.output.xml :as xml-output]
            [vip.data-processor.s3 :as s3])
  (:gen-class))

(def download-pipeline
  [t/read-edn-sqs-message
   t/assert-filename
   psql/start-run
   t/attach-sqlite-db
   t/download-from-s3
   zip/assoc-file
   zip/extracted-contents])

(def pipeline
  (concat download-pipeline
          [(data-spec/add-data-specs data-spec/data-specs)
           t/xml-csv-branch
           psql/store-public-id]
          db/validations
          xml-output/pipeline
          [s3/upload-to-s3]
          [psql/insert-validations
           psql/import-from-sqlite
           psql/store-stats
           cleanup/cleanup]))

(defn consume []
  (sqs/consume-messages (sqs/client)
                        (fn [message]
                          (q/publish {:initial-input message
                                      :status :started}
                                     "processing.started")
                          (let [result (pipeline/process pipeline message)]
                            (psql/complete-run result)
                            (log/info "New run completed:"
                                      (psql/get-run result)))
                          (q/publish {:initial-input message
                                      :status :complete}
                                     "processing.complete"))))

(defn -main [& args]
  (let [id (java.util.UUID/randomUUID)]
    (log/info "VIP Data Processor starting up. ID:" id)
    (q/initialize)
    (psql/initialize)
    (q/publish {:id id :event "starting"} "qa-engine.status")
    (let [consumer (consume)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (log/info "VIP Data Processor shutting down...")
                                   (q/publish {:id id :event "stopping"} "qa-engine.status")
                                   (future-cancel consumer))))
      (while true))))
