(ns vip.data-processor.validation.v5.polling-location
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-address-lines
  (util/validate-no-missing-elements :polling-location [:address-line]))
