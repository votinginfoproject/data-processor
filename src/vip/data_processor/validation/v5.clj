(ns vip.data-processor.validation.v5
  (:require [vip.data-processor.validation.v5.email :as email]
            [vip.data-processor.validation.v5.id :as id]))

(def validations
  [email/validate-emails
   id/validate-unique-ids])
