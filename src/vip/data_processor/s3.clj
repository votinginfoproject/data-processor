(ns vip.data-processor.s3
  (:require [aws.sdk.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]]
            [korma.core :as korma]
            [clojure.string :refer [join]])
  (:import [java.nio.file Files CopyOption StandardCopyOption]
           [java.nio.file.attribute FileAttribute]))

(defn- get-object [key]
  (s3/get-object (config :aws :creds)
                 (config :aws :s3 :bucket)
                 key))

(defn put-object [key value]
  (s3/put-object (config :aws :creds)
                 (config :aws :s3 :bucket)
                 key value))

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

(defn xml-filename [ctx]
  (let [fips (->> (get-in ctx [:tables :sources])
                  korma/select first :vip_id)
        formatted-fips (if (< (count fips) 2)
                         (format "%02d" (Integer/parseInt fips))
                         (if (< (count fips) 5)
                           (format "%05d" (Integer/parseInt fips))
                           fips))
        election-date (->> (get-in ctx [:tables :elections])
                           korma/select first :date)
        timestamp (.getTime (java.util.Date.))]
    (join "-" ["vipfeed" fips election-date timestamp])))

(defn upload-to-s3
  "Uploads the generated xml file to the specified S3 bucket."
  [ctx]
  (let [new-filename (xml-filename ctx)]
    (put-object (str "processed-feeds/" new-filename)
                (.toFile (:xml-output-file ctx)))
    ctx))
