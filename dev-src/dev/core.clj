(ns dev.core
  (:require [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [squishy.data-readers]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor :as data-processor]
            [vip.data-processor.cleanup :as cleanup]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.output.street-segments :as output-ss]
            [vip.data-processor.output.xml :as xml-output]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.db.v3-0 :as db.v3-0]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip])
  (:import [java.io File]
           [java.nio.file Files Paths StandardCopyOption]))

;; modify this to add flags to processing
(defn set-context
  [ctx]
  (-> ctx
      (assoc :post-process-street-segments? true)
      (assoc :save-zip-locally? false)))

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
  [set-context
   psql/start-run
   #(zip/assoc-file % (config [:max-zipfile-size] 3221225472)) ; 3GB
   zip/extracted-contents
   t/remove-invalid-extensions
   t/xml-csv-branch
   psql/analyze-xtv
   psql/store-spec-version
   psql/store-public-id
   psql/store-election-id
   psql/v5-summary-branch
   data-processor/add-validations
   output-ss/insert-street-segment-nodes
   errors/close-errors-chan
   errors/await-statistics
   save-zip-to-local-file
   psql/delete-from-xml-tree-values
   cleanup/cleanup])


(defn -main [filename]
  (psql/initialize)
  (let [file (if (zip/xml-file? filename)
               (Paths/get filename (into-array [""]))
               (File. filename))
        result (pipeline/process pipeline file)]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file)))
    (psql/complete-run result)))
