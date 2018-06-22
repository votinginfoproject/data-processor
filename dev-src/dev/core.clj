(ns dev.core
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.tools.logging :as log]
   [squishy.data-readers]
   [turbovote.resource-config :refer [config]]
   [vip.data-processor :as data-processor]
   [vip.data-processor.db.postgres :as psql]
   [vip.data-processor.errors :as errors]
   [vip.data-processor.output.street-segments :as output-ss]
   [vip.data-processor.output.xml :as xml-output]
   [vip.data-processor.pipeline :as pipeline]
   [vip.data-processor.s3 :as s3 :refer [zip-filename]]
   [vip.data-processor.validation.data-spec :as data-spec]
   [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
   [vip.data-processor.validation.db :as db]
   [vip.data-processor.validation.db.v3-0 :as db.v3-0]
   [vip.data-processor.validation.transforms :as t]
   [vip.data-processor.validation.zip :as zip]))

;; modify this to add flags to processing
(defn set-context
  [ctx]
  (assoc ctx :post-process-street-segments? true))

(def pipeline
  [set-context
   psql/start-run
   #(zip/assoc-file (config [:max-zipfile-size] 3221225472)) ; 3GB
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
   psql/delete-from-xml-tree-values])

(defn -main [filename]
  (psql/initialize)
  (let [file (if (zip/xml-file? filename)
               (java.nio.file.Paths/get filename (into-array [""]))
               (java.io.File. filename))
        result (pipeline/process pipeline file)]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file))
      (let [filename (zip-filename result)
            results (assoc result :generated-xml-filename filename)]
        (psql/complete-run results)))))
