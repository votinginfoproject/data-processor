(ns vip.data-processor.validation.v5.polling-location
  (:require [vip.data-processor.validation.v5.util :as util]
            [clojure.edn :as edn]))

(def validate-no-missing-address-lines
  (util/validate-no-missing-elements :polling-location [:address-line]))

(def validate-no-missing-latitudes
  (util/validate-no-missing-elements :lat-lng [:latitude]))

(def validate-no-missing-longitudes
  (util/validate-no-missing-elements :lat-lng [:longitude]))

(def validate-latitude
  (util/validate-elements :lat-lng
                          [:latitude]
                          (comp float? edn/read-string)
                          :errors :format))

(def validate-longitude
  (util/validate-elements :lat-lng
                          [:longitude]
                          (comp float? edn/read-string)
                          :errors :format))
