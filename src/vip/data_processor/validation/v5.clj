(ns vip.data-processor.validation.v5
  (:require [vip.data-processor.validation.v5.candidate :as candidate]
            [vip.data-processor.validation.v5.email :as email]
            [vip.data-processor.validation.v5.id :as id]))

(def validations
  [candidate/validate-no-missing-ballot-names
   candidate/validate-post-election-statuses
   email/validate-emails
   id/validate-unique-ids
   id/validate-no-missing-ids])
