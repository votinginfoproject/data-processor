(ns vip.data-processor.pipelines.csv.v5
  (:require [vip.data-processor.cleanup]
            [vip.data-processor.db.postgres]
            [vip.data-processor.db.translations.transformer]
            [vip.data-processor.db.v5]
            [vip.data-processor.errors]
            [vip.data-processor.errors.process]
            [vip.data-processor.output.xml-helpers]
            [vip.data-processor.output.street-segments]
            [vip.data-processor.output.tree-xml]
            [vip.data-processor.s3]
            [vip.data-processor.validation.csv]
            [vip.data-processor.validation.v5]))

(def pipeline
  (concat
   [vip.data-processor.validation.csv/error-on-missing-files
    vip.data-processor.validation.csv/remove-bad-filenames
    vip.data-processor.db.postgres/prep-v5-1-run
    vip.data-processor.db.v5/add-feed-indexes
    vip.data-processor.errors.process/process-v5-validations
    vip.data-processor.validation.csv/load-csvs]

    vip.data-processor.db.translations.transformer/transformers

    [vip.data-processor.output.xml-helpers/create-xml-file
     vip.data-processor.output.tree-xml/generate-xml-file
     vip.data-processor.db.postgres/analyze-xtv
     vip.data-processor.db.postgres/store-spec-version
     vip.data-processor.db.postgres/store-public-id
     vip.data-processor.db.postgres/store-election-id
     vip.data-processor.db.postgres/populate-locality-table
     vip.data-processor.db.postgres/populate-i18n-table
     vip.data-processor.db.postgres/populate-sources-table
     vip.data-processor.db.postgres/populate-elections-table]

    vip.data-processor.validation.v5/validations

    [vip.data-processor.output.street-segments/insert-street-segment-nodes
     vip.data-processor.errors/close-errors-chan
     vip.data-processor.errors/await-statistics
     vip.data-processor.s3/generate-xml-filename
     vip.data-processor.s3/upload-to-s3
     vip.data-processor.db.v5/drop-feed-indexes
     vip.data-processor.db.postgres/delete-from-xml-tree-values
     vip.data-processor.cleanup/cleanup]))
