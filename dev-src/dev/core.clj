(ns dev.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pprint]
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

(def v3-pipeline
  (concat db/validations
          db.v3-0/validations
          xml-output/pipeline
          [psql/insert-validations
           psql/import-from-sqlite
           psql/store-stats]))

(def pipeline
  [psql/start-run
   zip/assoc-file
   zip/extracted-contents
   (data-spec/add-data-specs v3-0/data-specs) ; TODO: dont't do this for non-3.0 feeds
   t/remove-invalid-extensions
   t/xml-csv-branch
   psql/store-public-id
   psql/store-election-id
   (fn [ctx]
     (if (= "3.0" (:spec-version ctx))
       (update ctx :pipeline concat v3-pipeline)
       ctx))])

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
