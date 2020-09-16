(ns vip.data-processor.queue
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [squishy.core :as sqs]
            [turbovote.resource-config :refer [config]]))

(defn ack-sqs-message
  "If we were configured with a delete-callback from SQS, call it, effectively
   acking that data-processor has received and kicked off processing.

   This will allow us to process very long feeds without spawning zombie
   processes after 12 hours, the maximum time we could keep a message
   checked out."
  [{:keys [delete-callback] :as ctx}]
  (when delete-callback (delete-callback))
  ctx)

(defn consume
  "Start a consumer that will pull messages from the SQS queue and
   send them to the message-handler for processing."
  [message-handler]
  (let [{:keys [access-key secret-key]} (config [:aws :creds])
        region (config [:aws :region])
        {:keys [queue fail-queue
                visibility-timeout]} (config [:aws :sqs])
        creds {:access-key access-key
               :access-secret secret-key
               :region region}
        opts (merge {:delete-callback true}
              (when visibility-timeout
                {:visibility-timeout visibility-timeout}))]
    (sqs/consume-messages creds queue fail-queue opts message-handler)))

(defn stop-consumer
  "Stop a message consumer, usually called during shutdown."
  [consumer-id]
  (sqs/stop-consumer consumer-id))

(defn sns-client
  [access-key secret-key region]
   (aws/client
    {:api                  :sns
     :region               region
     :credentials-provider (credentials/basic-credentials-provider
                            {:access-key-id     access-key
                             :secret-access-key secret-key})}))

(def default-sns-client
  "SNS Client configured by runtime environment"
  (delay (sns-client (config [:aws :creds :access-key])
                     (config [:aws :creds :secret-key])
                     (config [:aws :region]))))

(defn- publish
  [sns-client topic payload]
  (aws/invoke sns-client {:op :Publish
                          :request {:TopicArn topic
                                    :Message payload}}))

(defn publish-success
  "Publish a successful feed processing message to the topic."
  ([payload]
   (publish-success @default-sns-client
                    (config [:aws :sns :success-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing success to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))

(defn publish-failure
  "Publish a failed feed processing message to the topic."
  ([payload]
   (publish-failure @default-sns-client
                    (config [:aws :sns :failure-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing failure to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))
