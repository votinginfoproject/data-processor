(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [squishy.core :as sqs]
            [turbovote.resource-config :refer [config]]
            [korma.core :as korma]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.db.tree-statistics :as tree-stats]
            [vip.data-processor.validation.db.v3-0 :as db.v3-0]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.v5 :as v5-1-validations]
            [vip.data-processor.validation.xml :as xml]
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
   t/download-from-s3
   zip/assoc-file
   zip/extracted-contents])

(def v3-validation-pipeline
  (concat db/validations
          db.v3-0/validations
          xml-output/pipeline
          [psql/insert-validations
           psql/import-from-sqlite
           psql/store-stats]))

(def v5-1-validation-pipeline
  (concat v5-1-validations/validations
          [xml/load-xml-tree-validations
           tree-stats/store-tree-stats]))

(defn add-validations
  [{:keys [spec-version skip-validations?] :as ctx}]
  (if skip-validations?
    ctx
    (let [validations (condp = spec-version
                        "3.0" v3-validation-pipeline
                        "5.1" v5-1-validation-pipeline
                        nil)]
      (update ctx :pipeline (partial concat validations)))))

(def pipeline
  (concat download-pipeline
          [t/remove-invalid-extensions
           t/xml-csv-branch
           psql/store-spec-version
           psql/store-public-id
           psql/store-election-id
           add-validations
           s3/upload-to-s3
           cleanup/cleanup]))

(defn-traced process-message [message]
  (q/publish {:initial-input message
              :status :started}
             "processing.started")
  (let [result (pipeline/process pipeline message)]
    (psql/complete-run result)
    (log/info "New run completed:"
              (psql/get-run result))
    (let [exception (:exception result)
          completed-message (cond-> {:initial-input message
                                     :status :complete
                                     :public-id (:public-id result)}
                              exception (assoc :exception (.getMessage exception)))]
      (q/publish completed-message
                 "processing.complete")
      (when exception
        (throw exception)))))

(defn consume []
  (let [{:keys [access-key secret-key]} (config [:aws :creds])
        {:keys [region queue fail-queue]} (config [:aws :sqs])
        creds {:access-key access-key
               :access-secret secret-key
               :region region}]
    (sqs/consume-messages creds queue fail-queue process-message)))

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
      @consumer)))
