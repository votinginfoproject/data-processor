(ns dev.core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pprint]
            [vip.data-processor :as data-processor]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.output.xml :as xml-output]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.db.v3-0 :as db.v3-0]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.s3 :refer [zip-filename]]
            [squishy.data-readers]))


(def pipeline
  [psql/start-run
   zip/assoc-file
   zip/extracted-contents
   t/remove-invalid-extensions
   t/xml-csv-branch
   psql/analyze-xtv
   psql/store-spec-version
   psql/store-public-id
   psql/store-election-id
   psql/v5-summary-branch
   data-processor/add-validations
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
