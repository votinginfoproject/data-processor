(ns dev.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.output.xml :as xml-output]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.zip :as zip]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.s3 :refer [xml-filename]]))

(def pipeline
  (concat [zip/unzip
           zip/extracted-contents
           t/attach-sqlite-db
           (data-spec/add-data-specs data-spec/data-specs)
           t/xml-csv-branch]
          [psql/start-run]
          db/validations
          xml-output/pipeline))

(defn -main [zip-filename]
  (psql/initialize)
  (let [zip (java.io.File. zip-filename)
        result (pipeline/process pipeline zip)]
    (when-let [xml-output-file (:xml-output-file result)]
      (println "XML:" (.toString xml-output-file))
      (let [filename (xml-filename result)
            results (assoc result :generated-xml-filename filename)]
        (psql/complete-run results)))))
