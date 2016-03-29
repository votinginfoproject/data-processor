(ns vip.data-processor.validation.v5.internationalized-text
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-texts
  (util/validate-no-missing-elements :internationalized-text [:text]))
