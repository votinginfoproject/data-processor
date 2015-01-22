(ns vip.data-processor.s3
  (:require [aws.sdk.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]])
  (:import [java.nio.file Files CopyOption StandardCopyOption]
           [java.nio.file.attribute FileAttribute]))

(defn- get-object [key]
  (s3/get-object (config :aws :creds)
                 (config :aws :s3 :bucket)
                 key))

(def tmp-path-prefix "vip-data-processor")

(defn download
  "Downloads the file named `key` from the configured S3 bucket to a
  temporary file and returns that path."
  [key]
  (let [tmp-path (Files/createTempFile tmp-path-prefix key
                                       (into-array FileAttribute []))
        s3-object (get-object key)]
    (Files/copy (:content s3-object)
                tmp-path
                (into-array [StandardCopyOption/REPLACE_EXISTING]))
    tmp-path))
