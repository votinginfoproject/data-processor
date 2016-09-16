(ns vip.data-processor.validation.db.v3-0.fips
  (:require [korma.core :as korma]
            [vip.data-processor.validation.fips :refer [valid-fips?]]
            [vip.data-processor.errors :as errors]))

(defn validate-valid-source-vip-id [ctx]
  (let [sources (get-in ctx [:tables :sources])
        source (-> sources (korma/select (korma/fields :id :vip_id)) first)
        {:keys [id vip_id]} source]
    (if (valid-fips? vip_id)
      ctx
      (errors/add-errors ctx :errors :sources id :invalid-vip-id vip_id))))
