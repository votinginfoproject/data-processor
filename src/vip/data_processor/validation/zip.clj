(ns vip.data-processor.validation.zip
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]])
  (:import [java.util.zip ZipInputStream]
           [java.io File]
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

(defn open-zip-stream [file process-fn]
  (with-open [stream (ZipInputStream. (io/input-stream file))]
    (process-fn stream)))

(defn get-uncompressed-size
  "Given a File returns the uncompressed
  size as provided by the first (and presumably only) entry in the
  file."
  [file]
  (open-zip-stream file (fn [stream] (-> stream .getNextEntry .getSize))))

(defn extract-zip [file tmp-dir]
  (letfn [(entry-path [entry]
            (str tmp-dir File/separatorChar (.getName entry)))
          (extract [stream]
            (loop [entry (.getNextEntry stream)]
              (when entry
                (let [save-path (entry-path entry)
                      save-file (File. save-path)]
                  (if (.isDirectory entry)
                    (when-not (.exists save-file)
                      (.mkdirs save-file))
                    (let [parent-dir (.getParentFile save-file)]
                      (when-not (.exists parent-dir) (.mkdirs parent-dir))
                      (io/copy stream save-file))))
                (recur (.getNextEntry stream)))))]
    (open-zip-stream file extract)))

(defn unzip-file [file max-zipfile-size]
  (let [size (get-uncompressed-size file)]
    (if (> size (Long. max-zipfile-size))
      (-> (too-big-msg size max-zipfile-size)
          (ex-info {:msg (too-big-msg size max-zipfile-size)
                    :max-zipfile-size-exceeded true})
          throw)
      (let [tmp-dir (Files/createTempDirectory
                     "vip-extracted-feed"
                     (into-array FileAttribute []))]
        (extract-zip file tmp-dir)
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
