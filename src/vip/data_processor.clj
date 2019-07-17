(ns vip.data-processor
  (:require [clojure.tools.logging :as log]
            [vip.data-processor.queue :as q]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.pipelines :as pipelines]
            [vip.data-processor.pipelines.common :as common])
  (:gen-class))

(def pipeline
  (concat common/pipeline
          [pipelines/choose-pipeline]))

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
    (let [consumer-id (q/consume process-message)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (log/info "VIP Data Processor shutting down...")
                                   (q/stop-consumer consumer-id))))
      (log/info "SQS processing started")
      consumer-id)))
