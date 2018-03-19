(ns vip.data-processor.validation.db.v3-0
  (:require
   [vip.data-processor.validation.db.v3-0.admin-addresses :as admin-addresses]
   [vip.data-processor.validation.db.v3-0.candidate-addresses :as candidate-addresses]
   [vip.data-processor.validation.db.v3-0.fips :as fips]
   [vip.data-processor.validation.db.v3-0.jurisdiction-references :as jurisdiction-references]
   [vip.data-processor.validation.db.v3-0.precinct :as precinct]
   [vip.data-processor.validation.db.v3-0.street-segment :as street-segment]))

(def validations
  [admin-addresses/validate-addresses
   candidate-addresses/validate-addresses
   precinct/validate-no-missing-polling-locations
   street-segment/validate-no-overlapping-street-segments
   fips/validate-valid-source-vip-id
   jurisdiction-references/validate-jurisdiction-references])
