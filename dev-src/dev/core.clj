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
  [{:keys [xml-output-file save-zip-locally? gis-file-paths] :as ctx}]
  (if save-zip-locally?
    (let [zip-name (s3/zip-filename ctx)

          {:keys [zip-file zip-dir]}
          (s3/prepare-zip-file zip-name xml-output-file gis-file-paths)

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

(defn migrate
  "Apply any pending migrations."
  []
  (psql/migrate))

(defn pending
  "List any unapplied migrations."
  []
  (psql/pending))

(defn rollback
  "Rollback migrations. When called with no parameters, it rolls back
   the last applied migration. When called with a parameter, it will
   attempt to parse it into an integer, and if successful, rolls back
   that many migrations. If it's not an integer, it is assumed to be the
   name/id of a migration, and rolls back all applied migration up to but
   not including the id."
  ([]
   (psql/rollback))
  ([num-or-id]
   (psql/rollback num-or-id)))

(defn create
  "Creates new migration up/down files with the id as a suffix. Automatically
   addes the date in YYYYMMDD- format before the id, so if you intend to write
   multiple migrations on the same day that need to be applied in a particular
   order, it's ideal to start your id with 01-, 02-, 03-, etc"
  [id]
  (psql/create id))

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
                         :skip-upload? true
                         :save-zip-locally? true}
        result (time (pipeline/process pipeline initial-context))]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file)))
    (psql/complete-run result)))
