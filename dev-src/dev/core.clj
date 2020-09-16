(ns dev.core
  (:require [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.pipelines :as pipelines]
            [vip.data-processor.pipelines.common :as common]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.validation.transforms :as t])
  (:import [java.nio.file Files Paths StandardCopyOption]))

(defn save-zip-to-local-file
  "A dev pipeline step to save the resulting zip file to a local file at the
   end of processing, should you want to examine it. To activate it, flip
   the context keyword `:save-zip-locally?` to true above."
  [{:keys [xml-output-file save-zip-locally?] :as ctx}]
  (if save-zip-locally?
    (let [zip-name (s3/zip-filename ctx)

          {:keys [zip-file zip-dir]}
          (s3/prepare-zip-file zip-name xml-output-file)

          current-dir (-> ""
                          (Paths/get (into-array String []))
                          .toAbsolutePath)

          target-file (.resolve current-dir zip-name)]
      (Files/copy (.toPath zip-file) target-file
                  (into-array [StandardCopyOption/REPLACE_EXISTING]))
      (-> ctx
          (assoc :generated-xml-filename zip-name)
          (update :to-be-cleaned concat [zip-file zip-dir])))
    ctx))

(def pipeline
  [psql/start-run
   t/assert-file
   zip/process-file
   t/remove-invalid-extensions
   common/determine-format
   common/organize-source-files
   common/determine-spec
   pipelines/choose-pipeline
   save-zip-to-local-file
   cleanup/cleanup])

(defn -main
  "Runs a modified pipeline that skips downloading from S3 in favor of a :file
  that is provided on the command line. As such, it replaces the common pipeline
  with one suited for just this task. Alter any of the flags in the
  initial-context to turn on/off certain features:
  :post-process-street-segments? works the same as in a normal production
    pipeline to indicate if street segments should skip being added to the db
    (true) or if they should be added (false)
  :keep-feed-on-complete? should be set to true if you want the feed left
    in the database when the processing is done, so you can do query tuning
  :skip-upload? should be set to true if you do not want to upload to S3 when
    processing is complete"
  [filename]
  (psql/initialize)
  (let [file (java.nio.file.Paths/get filename (into-array [""]))
        initial-context {:file file
                         :post-process-street-segments? false
                         :keep-feed-on-complete? false
                         :skip-upload? true
                         :save-zip-locally? true}
        result (time (pipeline/process pipeline initial-context))]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file)))
    (psql/complete-run result)))

