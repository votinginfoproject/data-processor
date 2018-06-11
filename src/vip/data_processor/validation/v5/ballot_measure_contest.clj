(ns vip.data-processor.validation.v5.ballot-measure-contest
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-ballot-measure-types
  (util/validate-enum-elements :ballot-measure-type :errors))

(def validate-no-missing-types
  (util/validate-no-missing-elements :ballot-measure-contest [:electoral-district-id]))
