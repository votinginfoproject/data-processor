(ns dev.core
  (:require [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.pipelines :as pipelines]
            [vip.data-processor.pipelines.common :as common]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.validation.transforms :as t]))

;; modify this to add flags to processing
(defn set-context
  [ctx]
  (-> ctx
   (assoc :post-process-street-segments? true)
   (assoc :keep-feed-on-complete? false)
   (assoc :skip-upload? true)))

(def pipeline
  [set-context
   psql/start-run
   zip/assoc-file
   zip/extracted-contents
   t/remove-invalid-extensions
   common/determine-format
   common/determine-spec
   pipelines/choose-pipeline])

(defn -main [filename]
  (psql/initialize)
  (let [file (if (zip/xml-file? filename)
               (java.nio.file.Paths/get filename (into-array [""]))
               (java.io.File. filename))
        result (pipeline/process pipeline file)]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file))
      (psql/complete-run result))))
