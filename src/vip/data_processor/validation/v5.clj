(ns vip.data-processor.validation.v5
  (:require [vip.data-processor.validation.v5.email :as email]))

(def validations
  [email/validate-emails])
