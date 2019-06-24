(ns vip.data-processor.queue
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [turbovote.resource-config :refer [config]]))

(defn sns-client
  ([]
   (sns-client (config [:aws :creds :access-key])
               (config [:aws :creds :secret-key])
               (config [:aws :region])))
  ([access-key secret-key region]
   (aws/client
    {:api                  :sns
     :region               region
     :credentials-provider (credentials/basic-credentials-provider
                            {:access-key-id     access-key
                             :secret-access-key secret-key})})))

(defn- publish
  [sns-client topic payload]
  (aws/invoke sns-client {:op :Publish
                          :request {:TopicArn topic
                                    :Message payload}}))

(defn publish-success
  "Publish a successful feed processing message to the topic."
  ([payload]
   (publish-success (sns-client)
                    (config [:aws :sns :success-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing success to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))

(defn publish-failure
  "Publish a failed feed processing message to the topic."
  ([payload]
   (publish-failure (sns-client)
                    (config [:aws :sns :failure-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing failure to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))
