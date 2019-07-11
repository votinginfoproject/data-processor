(ns vip.data-processor.pipelines.common
  (:require [clojure.string :as s]
            [clojure.set :as set]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.queue :as q]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.validation.zip :as zip]))

(defn determine-format [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last s/lower-case))
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

(def pipeline
  [t/read-edn-sqs-message
   t/assert-filename
   psql/start-run
   q/ack-sqs-message
   t/download-from-s3
   zip/assoc-file
   zip/extracted-contents
   t/remove-invalid-extensions
   determine-format
   determine-spec])
