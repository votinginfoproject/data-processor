(ns vip.data-processor.validation.v5.internationalized-text
  (:require [vip.data-processor.validation.v5.util :as util]))

(defn validate-no-missing-texts [{:keys [import-id] :as ctx}]
  (let [validators (util/build-no-missing-validators
                    :internationalized-text
                    :text
                    import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))
