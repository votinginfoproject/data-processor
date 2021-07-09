(ns vip.data-processor.s3
  (:require [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]]
            [clojure.string :as str]
            [vip.data-processor.util :as util])
  (:import [java.io File ByteArrayOutputStream]
           [java.nio.file Files StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.security MessageDigest]
           [net.lingala.zip4j ZipFile]
           [net.lingala.zip4j.model ZipParameters]
           [net.lingala.zip4j.model.enums CompressionLevel]))

(defn get-object [key bucket]
  (s3/get-object (merge (config [:aws :creds])
                        {:endpoint (config [:aws :region])})
                 bucket
                 key))

(defn put-object [key value]
  (s3/put-object (merge (config [:aws :creds])
                        {:endpoint (config [:aws :region])})
                 (config [:aws :s3 :processed-bucket])
                 key value))

(def tmp-path-prefix "vip-data-processor")
(defn download
  "Downloads the file named `key` from the passed in S3 `bucket` to a
  temporary file and returns that path."
  [key bucket]
  (let [filename (-> key (str/split #"/") last)
        tmp-path (Files/createTempFile tmp-path-prefix filename
                                       (into-array FileAttribute []))
        s3-object (get-object key bucket)
        stream (:input-stream s3-object)]
    (try
      (Files/copy stream
                  tmp-path
                  (into-array [StandardCopyOption/REPLACE_EXISTING]))
      (finally (.close stream)))
    tmp-path))

(defn zip-filename
  [{:keys [output-file-basename]}]
  (str output-file-basename ".zip"))

(defn prepare-zip-file
  "Creates a temp directory, a new zip file in that directory, and adds
   the xml-output-file to the zip. It returns the zip file and directory
   so they can be used and cleaned up at the end of processing."
  [zip-name xml-output-file gis-file-paths]
  (let [zip-dir (Files/createTempDirectory tmp-path-prefix
                                           (into-array FileAttribute []))
        zip-file (File. (.toFile zip-dir) zip-name)
        zip (ZipFile. zip-file)
        zip-params (doto (ZipParameters.)
                     (.setCompressionLevel
                      CompressionLevel/NORMAL))
        xml-file (if (instance? File xml-output-file)
                   xml-output-file
                   (.toFile xml-output-file))
        gis-files (mapv #(.toFile %) gis-file-paths)
        files (java.util.ArrayList. (concat [xml-file] gis-files))]
    (.addFiles zip files zip-params)
    {:zip-dir zip-dir :zip-file zip-file}))

(defn checksum
  "Computes a SHA 512 checksum of the zip file"
  [file]
  (let [bytes'
        (with-open [xin (io/input-stream file)
                    xout (ByteArrayOutputStream.)]
          (io/copy xin xout)
          (.toByteArray xout))
        algorithm (MessageDigest/getInstance "sha-512")
        raw (.digest algorithm bytes')]
    (apply str (map #(format "%02x" %) raw))))

(defn upload-to-s3
  "Zips up the xml output file and uploads to the specified S3 bucket if there
   are no fatal errors."
  [{:keys [fatal-errors? xml-output-file gis-file-paths skip-upload?] :as ctx}]
  (let [zip-name (zip-filename ctx)
        {:keys [zip-dir zip-file]} (prepare-zip-file zip-name xml-output-file
                                                     gis-file-paths)
        upload? (not (or fatal-errors? skip-upload?))
        check (when upload?
                {:checksum (checksum zip-file)})]
    ;; We don't want to push this to S3 at all if we have fatal errors
    ;; as it may break ingestion and waste time.
    (when upload?
      (put-object zip-name zip-file))
    (-> ctx
        (merge check)
        (assoc :generated-xml-filename zip-name)
        (update :to-be-cleaned concat [zip-file zip-dir]))))
