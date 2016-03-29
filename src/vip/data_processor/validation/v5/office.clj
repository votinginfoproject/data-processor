(ns vip.data-processor.validation.v5.office
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/validate-no-missing-elements :office [:name]))

(def validate-no-missing-term-types
  (util/validate-no-missing-elements :office [:term :type]))

(def validate-term-types
  (util/validate-enum-elements :office-term-type :errors))
