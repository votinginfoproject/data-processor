(ns vip.data-processor.validation.v5.email
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]
            [vip.data-processor.errors :as errors]
            [clojure.tools.logging :as log]
            [vip.data-processor.validation.v5.util :as util]))

(defn validate-emails [{:keys [import-id] :as ctx}]
  (log/info "Validating emails")
  (let [emails (korma/select postgres/xml-tree-values
                 (korma/where {:results_id import-id})
                 (korma/where
                     (postgres/ltree-match
                      postgres/xml-tree-values :path
                      "VipObject.0.*.Email.*{1}")))]
    (reduce (fn [ctx row]
              (if (re-find (:check value-format/email)
                           (:value row))
                ctx
                (let [path (-> row :path .getValue)
                      parent-element-id (util/get-parent-element-id path import-id)]
                  (errors/add-v5-errors ctx
                                     :errors :email path :format parent-element-id
                                     (:value row)))))
            ctx emails)))
