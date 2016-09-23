(ns vip.data-processor.validation.zip
  (:require [clojure.tools.logging :as log])
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

(defn unzip-file [path]
  (let [zip-file (ZipFile. (str path))
        tmp-dir (Files/createTempDirectory "vip-extracted-feed"
                                           (into-array FileAttribute []))]
    (.extractAll zip-file (str tmp-dir))
    tmp-dir))

(defn assoc-file [ctx]
  (let [path (:input ctx)]
    (log/info "Starting to process from path:" (str path))
    (cond
      (zip-file? path) (assoc ctx :input (unzip-file path))
      (xml-file? path) (assoc ctx :input path)
      :else (assoc ctx :stop (str path " is not a zip or xml file!")))))

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
