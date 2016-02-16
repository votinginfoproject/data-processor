(ns vip.data-processor.validation.v5.email
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]))

(defn validate-emails [{:keys [import-id] :as ctx}]
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
                (assoc-in ctx
                          [:errors :email :format (-> row :path .getValue)]
                          (:value row))))
            ctx emails)))
