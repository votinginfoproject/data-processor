(ns vip.data-processor.validation.v5.ordered-contest
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-contest-ids
  (util/validate-no-missing-elements :ordered-contest [:contest-id]))
