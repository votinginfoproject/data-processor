(ns vip.data-processor.validation.v5.district-type
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate
  "Validates all DistrictType elements' formats."
  (util/validate-enum-elements :district-type))
