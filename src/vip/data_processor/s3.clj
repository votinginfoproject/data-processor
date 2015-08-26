(ns vip.data-processor.s3
  (:require [aws.sdk.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]]
            [korma.core :as korma]
            [clojure.string :refer [join]])
  (:import [java.io File]
           [java.nio.file Files CopyOption StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [net.lingala.zip4j.core ZipFile]
           [net.lingala.zip4j.model ZipParameters]
           [net.lingala.zip4j.util Zip4jConstants]))

(defn- get-object [key]
  (s3/get-object (config :aws :creds)
                 (config :aws :s3 :unprocessed-bucket)
                 key))

(defn put-object [key value]
  (s3/put-object (config :aws :creds)
                 (config :aws :s3 :processed-bucket)
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

(defn zip-filename [ctx]
  (let [fips (->> (get-in ctx [:tables :sources])
                  korma/select first :vip_id)
        formatted-fips (cond
                         (nil? fips) "XX"
                         (< (count fips) 2) (format "%02d" (Integer/parseInt fips))
                         (< (count fips) 5) (format "%05d" (Integer/parseInt fips))
                         :else fips)
        election-date (->> (get-in ctx [:tables :elections])
                           korma/select first :date)]
    (join "-" ["vipfeed" fips election-date])))

(defn upload-to-s3
  "Uploads the generated xml file to the specified S3 bucket."
  [ctx]
  (let [zip-name (str (zip-filename ctx) ".zip")
        xml-file (-> ctx :xml-output-file .toFile)
        zip-file (ZipFile. zip-name)
        zip-params (doto (ZipParameters.)
                     (.setCompressionLevel
                      Zip4jConstants/DEFLATE_LEVEL_NORMAL))]
    (.createZipFile zip-file xml-file zip-params)
    (put-object zip-name (File. zip-name))
    (assoc ctx :generated-xml-filename zip-name)))
