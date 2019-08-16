(ns vip.data-processor.validation.transforms
  (:require [clojure.string :as s]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.errors :as errors]))

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

(defn remove-invalid-extensions [ctx]
  (let [file-paths (:extracted-file-paths ctx)
        valid-extensions #{"csv" "txt" "xml"}
        invalid-fn (fn [file-path]
                     (not (get valid-extensions
                               (-> file-path
                                   .toFile
                                   .getName
                                   (s/split #"\.")
                                   last
                                   s/lower-case))))
        {valid-files false invalid-files true} (group-by invalid-fn file-paths)
        ctx (assoc ctx :valid-file-paths valid-files)]
    (if (seq invalid-files)
      (errors/add-errors ctx :warnings :import :global :invalid-extensions
                         (map #(.getName (.toFile %)) invalid-files))
      ctx)))
