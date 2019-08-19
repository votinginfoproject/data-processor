(ns vip.data-processor.pipelines.xml.v3
  (:require [vip.data-processor.cleanup]
            [vip.data-processor.db postgres sqlite]
            [vip.data-processor.errors]
            [vip.data-processor.errors.process]
            [vip.data-processor.output.xml]
            [vip.data-processor.output.xml-helpers]
            [vip.data-processor.validation.db]
            [vip.data-processor.validation.db.v3-0]
            [vip.data-processor.validation.xml]
            [vip.data-processor.s3]))

(def pipeline
  (concat
   [vip.data-processor.db.sqlite/attach-sqlite-db
    vip.data-processor.errors.process/process-v3-validations
    vip.data-processor.validation.xml/load-xml
    vip.data-processor.db.postgres/analyze-xtv
    vip.data-processor.db.postgres/store-spec-version
    vip.data-processor.db.postgres/store-public-id
    vip.data-processor.db.postgres/store-election-id]

   vip.data-processor.validation.db/validations
   vip.data-processor.validation.db.v3-0/validations

   [vip.data-processor.output.xml-helpers/create-xml-file
    vip.data-processor.output.xml/write-xml
    vip.data-processor.output.xml/validate-xml-output
    vip.data-processor.db.postgres/import-from-sqlite
    vip.data-processor.errors/close-errors-chan
    vip.data-processor.errors/await-statistics
    vip.data-processor.s3/generate-xml-filename
    vip.data-processor.s3/upload-to-s3
    vip.data-processor.cleanup/cleanup]))
