(ns vip.data-processor.pipelines.common
  (:require [clojure.string :as s]
            [clojure.set :as set]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.queue :as q]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.validation.zip :as zip]))

(defn determine-format [ctx]
  (let [file-extensions (->> ctx
                             :feed-file-paths
                             (map util/file-extension)
                             set)
        feed-format (condp set/superset? file-extensions
                      #{"txt" "csv"} :csv
                      #{"xml"} :xml
                      (throw (ex-info "File extensions don't match expectations, csv or txt for csv feed, or xml for xml feed" {:extensions file-extensions})))]
    (assoc ctx :format feed-format)))

(defn determine-spec
  [ctx]
  (if (= :csv (:format ctx))
    (csv/determine-spec-version ctx)
    (xml/determine-spec-version ctx)))

(defn organize-source-files
  "Places the valid source files in the context into a location according to the
  format of the feed, so that format specific steps can find them."
  [{:keys [feed-file-paths] :as ctx}]
  (if (= :xml (:format ctx))
    (assoc ctx :xml-source-file-path (first feed-file-paths))
    (assoc ctx :csv-source-file-paths feed-file-paths)))

(def initial-pipeline
  [t/assert-filename-and-bucket
   psql/start-run
   q/ack-sqs-message
   t/download-from-s3
   t/assert-file
   zip/process-file
   t/remove-invalid-extensions
   determine-format
   organize-source-files
   determine-spec])
