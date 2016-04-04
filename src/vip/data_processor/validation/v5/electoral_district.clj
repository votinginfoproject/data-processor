(ns vip.data-processor.validation.v5.electoral-district
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/validate-no-missing-elements :electoral-district [:name]))

(def validate-no-missing-types
  (util/validate-no-missing-elements :electoral-district [:type]))

;; type validation is done in vip.data-processor.validation.v5.district-type
