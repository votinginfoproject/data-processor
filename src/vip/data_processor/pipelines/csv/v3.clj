(ns vip.data-processor.pipelines.csv.v3
  (:require [vip.data-processor.validation.csv]
            [vip.data-processor.db.sqlite]
            [vip.data-processor.errors.process]
            [vip.data-processor.validation.csv.file-set]
            [vip.data-processor.db.postgres]
            [vip.data-processor.validation.db]
            [vip.data-processor.validation.db.v3-0]
            [vip.data-processor.output.xml-helpers]
            [vip.data-processor.output.xml]
            [vip.data-processor.errors]
            [vip.data-processor.s3]
            [vip.data-processor.cleanup]))

(def pipeline
  (concat
   [vip.data-processor.validation.csv/error-on-missing-files
    vip.data-processor.validation.csv/remove-bad-filenames
    vip.data-processor.db.sqlite/attach-sqlite-db
    vip.data-processor.errors.process/process-v3-validations
    vip.data-processor.validation.csv.file-set/validate-v3-0-file-dependencies
    vip.data-processor.validation.csv/load-csvs
    vip.data-processor.db.postgres/store-spec-version
    vip.data-processor.db.postgres/store-public-id
    vip.data-processor.db.postgres/store-election-id]

   vip.data-processor.validation.db/validations
   vip.data-processor.validation.db.v3-0/validations

   [vip.data-processor.output.xml-helpers/generate-file-basename
    vip.data-processor.output.xml-helpers/create-xml-file
    vip.data-processor.output.xml/write-xml
    vip.data-processor.output.xml/validate-xml-output
    vip.data-processor.db.postgres/import-from-sqlite
    vip.data-processor.errors/close-errors-chan
    vip.data-processor.errors/await-statistics]))
