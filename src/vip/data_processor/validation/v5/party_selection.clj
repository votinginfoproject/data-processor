(ns vip.data-processor.validation.v5.party-selection
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-party-ids
  (util/validate-no-missing-elements :party-selection [:party-id]))
