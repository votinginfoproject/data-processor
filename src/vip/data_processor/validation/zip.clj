(ns vip.data-processor.validation.zip
  (:require [clojure.tools.logging :as log]
            [turbovote.resource-config :refer [config]])
  (:import [net.lingala.zip4j.core ZipFile]
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
  "Given a net.lingala.zip4j.core.ZipFile returns the uncompressed
  size as provided by the first (and presumably only) header in the
  file."
  [zip-file]
  (.getUncompressedSize (first (.getFileHeaders zip-file))))

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
          tmp-dir))))

(defn assoc-file
  ([ctx]
   (assoc-file ctx (config [:max-zipfile-size] 3221225472)))
  ([ctx max-zipfile-size]
   (let [path (:input ctx)]
     (log/info "Starting to process from path:" (str path))
     (cond
       (zip-file? path)
       (try
         (assoc ctx :input (unzip-file path max-zipfile-size))
         (catch Exception e
           (if-let [max-zipfile-size-exceeded (ex-data e)]
             (assoc ctx :stop (:msg (ex-data e)))
             (throw e))))

       (xml-file? path) (assoc ctx :input path)

       :else (assoc ctx :stop (str path " is not a zip or xml file!"))))))

(defn zip-contents
  "Returns a seq of Files from the path of a extracted zip file. If
  the zip file included a directory named 'data' it will be the files
  in that directory. Otherwise, it will be the files from the
  top-level."
  [path]
  (let [data-dir (-> path
                     (.resolve "data")
                     .toFile)
        base-dir (if (.exists data-dir)
                   data-dir
                   (.toFile path))]
    (-> base-dir
        .listFiles
        seq)))

(defn extracted-contents [ctx]
  (let [path (:input ctx)]
    (if (xml-file? path)
      (assoc ctx :input
             [(.toFile path)])
      (let [files (zip-contents path)]
        (-> ctx
            (assoc :input files)
            (update :to-be-cleaned concat files))))))
