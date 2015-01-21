(ns vip.data-processor.s3
  (:require [aws.sdk.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]])
  (:import [java.io File]))

(defn- get-object [key]
  (s3/get-object (config :aws :creds)
                 (config :aws :s3 :bucket)
                 key))

(def tmp-file-prefix "vip-data-processor")

(defn download
  "Downloads the file named `key` from the configured S3 bucket to a
  temporary file and returns that file."
  [key]
  (let [tmp-file (File/createTempFile tmp-file-prefix key)
        s3-object (get-object key)]
    (with-open [w (io/writer tmp-file)
                r (io/reader (:content s3-object))]
      (doall (map #(.write w (str % "\n")) (line-seq r))))
    tmp-file))
