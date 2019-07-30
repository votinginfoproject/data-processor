(ns vip.data-processor.validation.zip
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [turbovote.resource-config :refer [config]])
  (:import [java.util.zip ZipInputStream]
           [java.io File]
           [java.nio.file Files StandardOpenOption]
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

(defn open-zip-stream [path process-fn]
  (with-open [file-input-stream
              (Files/newInputStream path
                                    (into-array [StandardOpenOption/READ]))
              zip-stream (ZipInputStream. file-input-stream)]
    (process-fn zip-stream)))

(defn get-uncompressed-size
  "Given a File returns the uncompressed
  size as provided by the first (and presumably only) entry in the
  file."
  [path]
  (open-zip-stream path (fn [stream] (-> stream .getNextEntry .getSize))))

(defn assert-max-zip-size
  "Asserts that if we have a zip file, that when unzipped it doesn't go above
  a maximum size, the default of which is 3GiB. Does nothing if the file is
  an uncompressed XML file, and stops processing if the file is neither of
  those things (a zip file or XML file)."
  ([ctx]
   (assert-max-zip-size ctx (config [:max-zipfile-size] 3221225472)))
  ([ctx max-zipfile-size]
   (let [file-path (:file ctx)]
     (cond
       (zip-file? file-path)
       (let [size (get-uncompressed-size file-path)]
         (if (> size (Long. max-zipfile-size))
           (assoc ctx :stop (too-big-msg size max-zipfile-size))
           ctx))

       (xml-file? file-path) ctx

       :else (assoc ctx :stop (str file-path " is not a zip or xml file!"))))))

(defn extract-zip [zip-path tmp-dir]
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
    (open-zip-stream zip-path extract)))

(defn unzip-path-file [zip-path]
  (let [tmp-dir (Files/createTempDirectory
                 "vip-extracted-feed"
                 (into-array FileAttribute []))]
    (extract-zip zip-path tmp-dir)
    tmp-dir))

(defn resolve-zip-contents
  "Returns a seq of Paths from the path of a extracted zip file. If
  the zip file included a directory named 'data' it will be the file Paths
  in that directory. Otherwise, it will be the file Paths from the
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

(defn extract-contents
  "If the :file is a zip file, uncompresses it and finds the main collection of
  files by the `resolve-zip-contents` function. Otherwise it is an XML file and
  it places that in a collection at :extracted-file-paths, the same place it
  puts the resolved zip contents."
  [ctx]
  (let [path (:file ctx)]
    (if (xml-file? path)
      (assoc ctx :extracted-file-paths
             [path])
      (let [extracted-dir (unzip-path-file path)
            files (resolve-zip-contents extracted-dir)]
        (-> ctx
            (assoc :extracted-file-paths files)
            (update :to-be-cleaned concat files)
            (update :to-be-cleaned concat extracted-dir))))))
