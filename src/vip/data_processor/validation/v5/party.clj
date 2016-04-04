(ns vip.data-processor.validation.v5.party
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-colors
  (util/validate-elements :html-color-string
                          #(re-matches #"\A[0-9a-f]{6}\z" %)
                          :errors :format))
