(ns vip.data-processor.validation.v5.ballot-measure-contest
  (:require [vip.data-processor.validation.v5.util :as util]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

(def validate-ballot-measure-types
  (util/validate-enum-elements :ballot-measure-type :errors))

(def validate-no-missing-types
  (util/validate-no-missing-elements :ballot-measure-contest [:type]))
