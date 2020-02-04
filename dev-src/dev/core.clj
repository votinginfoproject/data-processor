(ns dev.core
  (:require [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.pipelines :as pipelines]
            [vip.data-processor.pipelines.common :as common]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.validation.transforms :as t]))

(def pipeline
  [psql/start-run
   t/assert-file
   zip/assert-max-zip-size
   zip/extract-contents
   t/remove-invalid-extensions
   common/determine-format
   common/organize-source-files
   common/determine-spec
   pipelines/choose-pipeline])

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
                         :post-process-street-segments? true
                         :keep-feed-on-complete? false
                         :skip-upload? true}
        result (time (pipeline/process pipeline initial-context))]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file))
      (psql/complete-run result))))
