(ns vip.data-processor.validation.v5.locality
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/validate-no-missing-elements :locality [:name]))

(def validate-no-missing-state-ids
  (util/validate-no-missing-elements :locality [:state-id]))

;; type validation is done in vip.data-processor.validation.v5.district-type
