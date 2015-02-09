(ns vip.data-processor.queue
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [turbovote.resource-config :refer [config]]))

(def rabbit-connection (atom nil))
(def rabbit-channel (atom nil))
(def qa-engine-exchange "qa-engine")

(defn initialize []
  (let [max-retries 5]
    (loop [attempt 1]
      (try
        (reset! rabbit-connection
                (rmq/connect (or (config :rabbit-mq :connection-params)
                                 {})))
        (log/info "RabbitMQ connected.")
        (catch Throwable t
          (log/warn "RabbitMQ not available:" (.getMessage t) "attempt:" attempt)))
      (when (nil? @rabbit-connection)
        (if (< attempt max-retries)
          (do (Thread/sleep (* attempt 1000))
              (recur (inc attempt)))
          (do (log/error "Connecting to RabbitMQ failed. Bailing.")
            (throw (ex-info "Connecting to RabbitMQ failed" {:attemts attempt})))))))
  (reset! rabbit-channel
          (let [ch (lch/open @rabbit-connection)]
            (le/topic ch qa-engine-exchange {:durable false :auto-delete true})
            (log/info "RabbitMQ topic set.")
            ch)))

(defn publish
  "Publish a message to the qa-engine topic exchange on the given
  routing-key. The payload will be converted to EDN."
  [payload routing-key]
  (log/debug routing-key "-" (pr-str payload))
  (lb/publish @rabbit-channel
              qa-engine-exchange
              routing-key
              (pr-str payload)
              {:content-type "application/edn" :type "qa-engine.event"}))
