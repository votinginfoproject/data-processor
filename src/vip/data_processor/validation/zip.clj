(ns vip.data-processor.validation.zip
  (:import [net.lingala.zip4j.core ZipFile]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

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

(defn unzip [ctx]
  (let [path (:input ctx)]
    (if (zip-file? path)
      (assoc ctx :input (unzip-file path))
      (assoc ctx :stop (str path " is not a zip file!")))))
