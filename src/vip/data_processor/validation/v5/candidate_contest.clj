(ns vip.data-processor.validation.v5.candidate-contest
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-types
  (util/validate-no-missing-elements :candidate-contest [:electoral-district-id]))
