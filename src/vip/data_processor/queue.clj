(ns vip.data-processor.queue
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [turbovote.resource-config :refer [config]]))

(defn sns-client
  [access-key secret-key region]
   (aws/client
    {:api                  :sns
     :region               region
     :credentials-provider (credentials/basic-credentials-provider
                            {:access-key-id     access-key
                             :secret-access-key secret-key})}))

(def sns-client-memo (memoize sns-client))

(defn default-sns-client
  []
   ;; memoize the default sns-client construction, so that
   ;; we can avoid creating extra thread pools over time.
   ;; see https://github.com/cognitect-labs/aws-api/issues/80
   (memoize (sns-client (config [:aws :creds :access-key])
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
   (publish-success (default-sns-client)
                    (config [:aws :sns :success-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing success to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))

(defn publish-failure
  "Publish a failed feed processing message to the topic."
  ([payload]
   (publish-failure (default-sns-client)
                    (config [:aws :sns :failure-topic-arn])
                    payload))
  ([sns-client topic payload]
   (let [json-payload (json/write-str payload)]
     (log/debug "publishing failure to " topic " with payload " json-payload)
     (publish sns-client topic json-payload))))
