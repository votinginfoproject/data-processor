(ns vip.data-processor.validation.zip
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
    (cond
      (zip-file? path) (assoc ctx :input (unzip-file path))
      (xml-file? path) (assoc ctx :input path)
      :else (assoc ctx :stop (str path " is not a zip or xml file!")))))

(defn extracted-contents [ctx]
  (let [path (:input ctx)]
    (if (xml-file? path)
      (assoc ctx :input
             [(.toFile path)])
      (let [files (-> path
                      (.resolve "data")
                      .toFile
                      .listFiles
                      seq)]
        (-> ctx
            (assoc :input files)
            (update :to-be-cleaned concat files))))))
