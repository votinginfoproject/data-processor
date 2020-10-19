(ns vip.data-processor.validation.zip
  (:require [clojure.tools.logging :as log]
            [turbovote.resource-config :refer [config]])
  (:import [net.lingala.zip4j ZipFile]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn xml-file? [path]
  (-> path
      str
      (.endsWith ".xml")))

(defn zip-file? [path]
  (-> path
      str
      (.endsWith ".zip")))

(defn too-big-msg
  [size max-zipfile-size]
  (str "Zip file size is " size
       ", which is greater than the max-zipfile-size " max-zipfile-size))

(defn get-uncompressed-size
  "Given a net.lingala.zip4j.ZipFile returns the uncompressed
  size as provided by the first (and presumably only) entry in the
  file."
  [zip-file]
  (letfn [(sum-header-size [sum header]
            (+ sum (.getUncompressedSize header)))]
    (reduce sum-header-size 0 (.getFileHeaders zip-file))))

(defn find-files
  "Returns a seq of Files from the path of a extracted zip file. If
  the zip file included a directory named 'data' it will be the files
  in that directory. Otherwise, it will be the files from the
  top-level."
  [path]
  (let [data-dir (-> path
                     (.resolve "data"))
        base-dir (if (.exists (.toFile data-dir))
                   data-dir
                   path)]
    (-> base-dir
        Files/list
        .iterator
        iterator-seq)))

(defn unzip-file [path max-zipfile-size]
  (let [zip-file (ZipFile. (str path))
        size (get-uncompressed-size zip-file)
        tmp-dir (Files/createTempDirectory "vip-extracted-feed"
                                           (into-array FileAttribute []))]
    (if (> size (Long. max-zipfile-size))
      (-> (too-big-msg size max-zipfile-size)
          (ex-info {:msg (too-big-msg size max-zipfile-size)
                    :max-zipfile-size-exceeded true})
          throw)
      (do (.extractAll zip-file (str tmp-dir))
          {:dir tmp-dir
           :files (find-files tmp-dir)}))))

(defn process-file
  "Given a context with the incoming source file at :input,
   conditionally unzips the contents (if it is a zip file)
   and puts them at :file. Also checks to see that the zip
   contents are not over the max-zipfile-size, if they are then
   it stops processing. If the :input just happens to be an unzipped
   xml file, it also just places that at :file and continues on."
  ([ctx]
   (process-file ctx (config [:max-zipfile-size] 3221225472)))
  ([ctx max-zipfile-size]
   (let [path (:file ctx)]
     (log/info "Starting to process from path:" (str path))
     (cond
       (zip-file? path)
       (try
         (let [{:keys [dir files]} (unzip-file path max-zipfile-size)]
           (-> ctx
               (assoc :file-type :zip)
               (assoc :extracted-file-paths files)
               (update :to-be-cleaned concat files)
               (update :to-be-cleaned concat dir)))
         (catch Exception e
           (if-let [max-zipfile-size-exceeded (ex-data e)]
             (assoc ctx :stop (:msg (ex-data e)))
             (throw e))))

       (xml-file? path)
       (-> ctx
           (assoc :file-type :xml)
           (assoc :extracted-file-paths [path]))

       :else (assoc ctx :stop (str path " is not a zip or xml file!"))))))
