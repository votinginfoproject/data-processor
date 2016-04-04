(ns vip.data-processor.validation.v5.street-segment
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.string :as str]))

(util/validate-no-missing-values :street-segment
                                 [:odd-even-both]
                                 [:city]
                                 [:state]
                                 [:street-name]
                                 [:zip])

(def validate-odd-even-both-value
  (util/validate-enum-elements :oeb-enum :errors))
