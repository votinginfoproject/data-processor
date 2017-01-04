(ns vip.data-processor.validation.v5
  (:require [vip.data-processor.validation.v5.ballot-measure-contest :as ballot-measure-contest]
            [vip.data-processor.validation.v5.candidate :as candidate]
            [vip.data-processor.validation.v5.email :as email]
            [vip.data-processor.validation.v5.external-identifiers :as external-identifiers]
            [vip.data-processor.validation.v5.id :as id]
            [vip.data-processor.validation.v5.precinct :as precinct]
            [vip.data-processor.validation.v5.source :as source]
            [vip.data-processor.validation.v5.retention-contest :as retention-contest]
            [vip.data-processor.validation.v5.state :as state]
            [vip.data-processor.validation.v5.hours-open :as hours-open]
            [vip.data-processor.validation.v5.electoral-district :as electoral-district]
            [vip.data-processor.validation.v5.election-administration :as election-admin]
            [vip.data-processor.validation.v5.election :as election]
            [vip.data-processor.validation.v5.locality :as locality]
            [vip.data-processor.validation.v5.internationalized-text :as intl-text]
            [vip.data-processor.validation.v5.district-type :as district-type]
            [vip.data-processor.validation.v5.office :as office]
            [vip.data-processor.validation.v5.party :as party]
            [vip.data-processor.validation.v5.party-selection :as party-selection]
            [vip.data-processor.validation.v5.ordered-contest :as ordered-contest]
            [vip.data-processor.validation.v5.street-segment :as street-segment]
            [vip.data-processor.validation.v5.boolean :as boolean-validation]
            [vip.data-processor.validation.v5.polling-location :as polling-location]))

(def validations
  [ballot-measure-contest/validate-ballot-measure-types
   ballot-measure-contest/validate-no-missing-types
   candidate/validate-no-missing-ballot-names
   candidate/validate-pre-election-statuses
   candidate/validate-post-election-statuses
   email/validate-emails
   external-identifiers/validate-no-missing-types
   external-identifiers/validate-no-missing-values
   id/validate-unique-ids
   id/validate-no-missing-ids
   id/validate-idref-references
   id/validate-idrefs-references
   precinct/validate-no-missing-names
   precinct/validate-no-missing-locality-ids
   source/validate-one-source
   source/validate-name
   source/validate-date-time
   source/validate-vip-id
   source/validate-vip-id-valid-fips
   retention-contest/validate-no-missing-candidate-ids
   state/validate-no-missing-names
   hours-open/validate-times
   hours-open/validate-dates
   electoral-district/validate-no-missing-names
   electoral-district/validate-no-missing-types
   election-admin/validate-no-missing-departments
   election-admin/validate-voter-service-type-format
   election/validate-one-election
   election/validate-date
   election/validate-state-id
   locality/validate-no-missing-names
   locality/validate-no-missing-state-ids
   intl-text/validate-no-missing-texts
   district-type/validate
   office/validate-no-missing-names
   office/validate-no-missing-term-types
   office/validate-term-types
   party/validate-colors
   party-selection/validate-no-missing-party-ids
   ordered-contest/validate-no-missing-contest-ids
   street-segment/validate-no-missing-odd-even-both
   street-segment/validate-odd-even-both-value
   street-segment/validate-no-missing-city
   street-segment/validate-no-missing-state
   street-segment/validate-no-missing-zip
   street-segment/validate-no-street-segment-overlaps
   polling-location/validate-no-missing-address-lines
   polling-location/validate-no-missing-latitudes
   polling-location/validate-no-missing-longitudes
   polling-location/validate-latitude
   polling-location/validate-longitude
   boolean-validation/validate-booleans])
