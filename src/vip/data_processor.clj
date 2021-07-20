(ns vip.data-processor
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.queue :as q]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.pipelines :as pipelines]
            [vip.data-processor.pipelines.common :as common]
            [vip.data-processor.s3 :as s3])
  (:gen-class))

(def pipeline
  (concat common/initial-pipeline
          [pipelines/choose-pipeline
           s3/upload-to-s3
           cleanup/cleanup]))

(defn process-message
  ([raw-message]
   (process-message raw-message nil))
  ([raw-message delete-callback]
   (log/info "processing message" (pr-str raw-message))
   (let [message (edn/read-string (:body raw-message))
         initial-context (merge {:skip-validations? false
                                 :post-process-street-segments? false}
                                message)
         result (pipeline/process pipeline initial-context delete-callback)
         exception (:exception result)
         stop (:stop result)
         completed-message {:initialInput message
                            :status "complete"
                            :publicId (:public-id result)}]
     (psql/complete-run result)
     (log/info "New run completed:"
               (psql/get-run result))
     (when stop
       (q/publish-failure (merge completed-message
                                 {:stop stop})))
     (if exception
       (q/publish-failure (merge completed-message
                                 {:exception (.getMessage exception)}))
       (q/publish-success (merge completed-message
                                 (select-keys result [:checksum]))))
     (when exception
       (throw exception)))))

(defn -main [& args]
  (let [id (java.util.UUID/randomUUID)]
    (log/info "VIP Data Processor starting up. ID:" id)
    (psql/initialize)
    (let [consumer-id (q/consume process-message)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (log/info "VIP Data Processor shutting down...")
                                   (q/stop-consumer consumer-id))))
      (log/info "SQS processing started")
      consumer-id)))
