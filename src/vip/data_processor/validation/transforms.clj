(ns vip.data-processor.validation.transforms
  (:require [clojure.string :as s]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.util :as util]))

(defn assert-filename-and-bucket
  "Asserts that we have a filename and bucket in the context
  so we know where to download the file from."
  [ctx]
  (let [filename (get ctx :filename nil)
        bucket (get ctx :bucket nil)]
    (if (some s/blank? [filename bucket])
      (assoc ctx :stop "No filename or bucket!")
      ctx)))

(defn download-from-s3
  "Downloads the file from the bucket in S3"
  [ctx]
  (let [filename (:filename ctx)
        bucket (:bucket ctx)
        file (s3/download filename bucket)]
    (-> ctx
        (update :to-be-cleaned conj file)
        (assoc :file file))))

(defn assert-file
  "Asserts that somehow we have a file to process, whether it was downloaded
  or provided directly to the pipeline."
  [ctx]
  (if-let [file (get ctx :file nil)]
    ctx
    (assoc ctx :stop "No file!")))

(def feed-extensions #{"csv" "txt" "xml"})
(def gis-extensions #{"shp"})

(defn sort-file
  "Returns one of 3 values for the file:
   :feed    - the file is a feed file (XML or CSV/TXT)
   :gis     - the file is a GIS related file (shapefile)
   :invalid - everything else"
  [file-path]
  (let [extension (util/file-extension file-path)]
    (cond
      (contains? feed-extensions extension) :feed
      (contains? gis-extensions  extension) :gis
      :else :invalid)))

(defn remove-invalid-extensions [ctx]
  (let [file-paths (:extracted-file-paths ctx)
        {feed-file-paths    :feed
         gis-file-paths     :gis
         invalid-file-paths :invalid} (group-by sort-file file-paths)
        ctx (assoc ctx :feed-file-paths feed-file-paths)
        ctx (if (seq gis-file-paths)
              (assoc ctx :gis-file-paths gis-file-paths) ctx)]
    (if (seq invalid-file-paths)
      (errors/add-errors ctx :warnings :import :global :invalid-extensions
                         (map #(.getName (.toFile %)) invalid-file-paths))
      ctx)))
