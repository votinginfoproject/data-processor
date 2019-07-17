(ns vip.data-processor.pipelines.xml.v5
  (:require [vip.data-processor.cleanup]
            [vip.data-processor.db.postgres]
            [vip.data-processor.errors]
            [vip.data-processor.errors.process]
            [vip.data-processor.s3]
            [vip.data-processor.validation.v5]
            [vip.data-processor.validation.xml]
            [vip.data-processor.validation.xml.v5]))

(def pipeline
  (concat
   [vip.data-processor.errors.process/process-v5-validations
    vip.data-processor.validation.xml/load-xml-ltree
    vip.data-processor.validation.xml.v5/load-xml-street-segments
    vip.data-processor.validation.xml/set-input-as-xml-output-file
    vip.data-processor.db.postgres/analyze-xtv
    vip.data-processor.db.postgres/store-spec-version
    vip.data-processor.db.postgres/store-election-id
    vip.data-processor.db.postgres/populate-locality-table
    vip.data-processor.db.postgres/populate-i18n-table
    vip.data-processor.db.postgres/populate-sources-table
    vip.data-processor.db.postgres/populate-elections-table]
   vip.data-processor.validation.v5/validations
   [vip.data-processor.errors/close-errors-chan
    vip.data-processor.errors/await-statistics
    vip.data-processor.s3/generate-xml-filename
    vip.data-processor.s3/upload-to-s3
    vip.data-processor.db.postgres/delete-from-xml-tree-values
    vip.data-processor.cleanup/cleanup]))
