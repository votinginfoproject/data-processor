(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.output.street-segments :as output-ss]
            [vip.data-processor.output.xml :as xml-output]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.queue :as q]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.db.v3-0 :as db.v3-0]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.v5 :as v5-1-validations]
            [vip.data-processor.validation.zip :as zip])
  (:gen-class))

(def download-pipeline
  [t/read-edn-sqs-message
   t/assert-filename
   psql/start-run
   q/ack-sqs-message
   t/download-from-s3
   #(zip/assoc-file % (config [:max-zipfile-size] 3221225472)) ; 3GB default
   zip/extracted-contents])

(def v3-validation-pipeline
  (concat db/validations
          db.v3-0/validations
          xml-output/pipeline
          [psql/import-from-sqlite]))

(def v5-1-validation-pipeline
  v5-1-validations/validations)

(defn add-validations
  [{:keys [spec-version skip-validations?] :as ctx}]
  (if skip-validations?
    (do
      (log/info "Skipping validations")
      ctx)
    (let [validations (condp = (util/version-without-patch @spec-version)
                        "3.0" v3-validation-pipeline
                        "5.1" v5-1-validation-pipeline
                        nil)]
      (log/info "Adding validations for" (pr-str @spec-version))
      (update ctx :pipeline (partial concat validations)))))

(def pipeline
  (concat download-pipeline
          [t/remove-invalid-extensions
           t/xml-csv-branch
           psql/analyze-xtv
           psql/store-spec-version
           psql/store-public-id
           psql/store-election-id
           psql/v5-summary-branch
           add-validations
           output-ss/insert-street-segment-nodes
           errors/close-errors-chan
           errors/await-statistics
           s3/upload-to-s3
           psql/delete-from-xml-tree-values
           cleanup/cleanup]))

(defn process-message
  ([message]
   (process-message message nil))
  ([message delete-callback]
   (log/info "processing message" (pr-str message))
   (let [result (pipeline/process pipeline message delete-callback)
         exception (:exception result)
         completed-message {:initialInput message
                            :status "complete"
                            :publicId (:public-id result)}]
     (psql/complete-run result)
     (log/info "New run completed:"
               (psql/get-run result))
     (if exception
       (q/publish-failure (merge completed-message
                                 {:exception (.getMessage exception)}))
       (q/publish-success completed-message))
     (when exception
       (throw exception)))))

(defn -main [& args]
  (let [id (java.util.UUID/randomUUID)]
    (log/info "VIP Data Processor starting up. ID:" id)
    (psql/initialize)
    (let [consumer-id (q/consume)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (log/info "VIP Data Processor shutting down...")
                                   (q/stop-consumer consumer-id))))
      (log/info "SQS processing started")
      consumer-id)))
