(ns vip.data-processor.s3
  (:require [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]]
            [korma.core :as korma]
            [clojure.string :as str]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.util :as util])
  (:import [java.io File FileOutputStream FileInputStream]
           [java.nio.file Files Path
            CopyOption StandardCopyOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipOutputStream ZipEntry]))

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

(defn format-fips [fips]
  (cond
    (empty? fips) "XX"
    (< (count fips) 3) (format "%02d" (Integer/parseInt fips))
    (< (count fips) 5) (format "%05d" (Integer/parseInt fips))
    :else fips))

(defn format-state
  [state]
  (if (empty? state)
    "YY"
    (clojure.string/replace (clojure.string/trim state) #"\s" "-")))

(defn zip-filename* [fips state election-date]
  (let [fips (format-fips fips)
        state (format-state state)
        date (util/format-date election-date)]
    (str (str/join "-" ["vipfeed" fips state date]) ".zip")))

(defn zip-filename
  [{:keys [spec-version tables import-id] :as ctx}]
  (condp = (util/version-without-patch @spec-version)
    "3.0"
    (let [fips (-> tables
                   :sources
                   korma/select
                   first
                   :vip_id)
          state (-> tables
                    :states
                    korma/select
                    first
                    :name)
          election-date (-> tables
                            :elections
                            korma/select
                            first
                            :date)]
      (zip-filename* fips state election-date))

    "5.1"
    (let [fips (postgres/find-value-for-simple-path
                import-id "VipObject.Source.VipId")
          state (postgres/find-value-for-simple-path
                 import-id "VipObject.State.Name")
          election-date (postgres/find-value-for-simple-path
                         import-id "VipObject.Election.Date")]
      (zip-filename* fips state election-date))))

(defn generate-xml-filename
  [ctx]
  (assoc ctx :generated-xml-filename (zip-filename ctx)))

(defn zip-file
  "Given a directory, a zip file name and an unzipped source file, creates
   a zipped file with the name and zipped contents of the source file."
  [zip-dir zip-name source-file-path]
  (let [zip-file-path (.resolve zip-dir zip-name)]
    (with-open [fos (Files/newOutputStream
                     zip-file-path
                     (into-array [StandardOpenOption/CREATE]))
                zos (ZipOutputStream. fos)
                fis (Files/newInputStream source-file-path
                        (into-array [StandardOpenOption/READ]))]
      (let [zip-entry (-> source-file-path .toFile .getName ZipEntry.)]
        (.putNextEntry zos zip-entry)
        (io/copy fis zos)))
    (.toFile zip-file-path)))

(defn upload-to-s3
  "Uploads the generated xml file to the specified S3 bucket."
  [{:keys [fatal-errors? xml-output-file generated-xml-filename] :as ctx}]
  (if-not (get ctx :skip-upload? false)
    (let [zip-dir (Files/createTempDirectory tmp-path-prefix
                                             (into-array FileAttribute []))
          zip-file (zip-file zip-dir generated-xml-filename xml-output-file)]
      ;; We don't want to push this to S3 at all if we have fatal errors
      ;; as it may break ingestion and waste time.
      (when-not fatal-errors?
        (put-object generated-xml-filename zip-file))
      (-> ctx
          (update :to-be-cleaned concat [zip-file zip-dir])))
    ctx))
