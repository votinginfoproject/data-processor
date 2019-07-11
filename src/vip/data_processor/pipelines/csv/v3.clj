(ns vip.data-processor.pipelines.csv.v3)

(def pipeline
  [vip.data-processor.validation.csv/error-on-missing-files
   vip.data-processor.validation.csv/remove-bad-filenames
   vip.data-processor.db.sqlite/attach-sqlite-db
   vip.data-processor.errors.process/process-v3-validations
   vip.data-processor.validation.csv.file-set/validate-v3-0-file-dependencies
   vip.data-processor.validation.csv/load-csvs
   vip.data-processor.db.postgres/store-spec-version
   vip.data-processor.db.postgres/store-election-id
   vip.data-processor.validation.db/validate-no-duplicated-ids
   vip.data-processor.validation.db/validate-no-duplicated-rows
   vip.data-processor.validation.db/validate-references
   vip.data-processor.validation.db/validate-one-record-limit
   vip.data-processor.validation.db/validate-no-unreferenced-rows
   vip.data-processor.validation.db.v3-0.admin-addresses/validate-addresses
   vip.data-processor.validation.db.v3-0.candidate-addresses/validate-addresses
   vip.data-processor.validation.db.v3-0.precinct/validate-no-missing-polling-locations
   vip.data-processor.validation.db.v3-0.street-segment/validate-no-overlapping-street-segments
   vip.data-processor.validation.db.v3-0.fips/validate-valid-source-vip-id
   vip.data-processor.validation.db.v3-0.jurisdiction-references/validate-jurisdiction-references
   vip.data-processor.output.xml-helpers/create-xml-file
   vip.data-processor.output.xml/write-xml
   vip.data-processor.output.xml/validate-xml-output
   vip.data-processor.db.postgres/import-from-sqlite
   vip.data-processor.errors/close-errors-chan
   vip.data-processor.errors/await-statistics
   vip.data-processor.s3/upload-to-s3
   vip.data-processor.cleanup/cleanup])
