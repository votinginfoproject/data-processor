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
    (if (zip-file? path)
      (assoc ctx :input (unzip-file path))
      (if (xml-file? path)
        (assoc ctx :input path)
        (assoc ctx :stop (str path " is not a zip file!"))))))

(defn extracted-contents [ctx]
  (let [path (:input ctx)]
    (if (xml-file? path)
      (assoc ctx :input
             (seq [(.toFile path)]))
      (assoc ctx :input
             (-> path
                 (.resolve "data")
                 .toFile
                 .listFiles
                 seq)))))
