(ns vip.data-processor.errors.process
  (:require [utility-works.async :as util-async]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.db.tree-statistics :as tree-stats]))

(defn batch-process-fn [validations-table
                        validations-transform
                        stats-fn]
  (fn [{:keys [errors-chan] :as ctx}]
    (let [processing-chan (util-async/batch-process
                           errors-chan
                           (fn [errors]
                             (psql/bulk-import
                              ctx
                              validations-table
                              (map validations-transform errors)))
                           500
                           5000
                           (partial stats-fn ctx))]
      (assoc ctx :processing-chan processing-chan))))

(def process-v3-validations
  (batch-process-fn psql/validations
                    psql/validation-value
                    psql/store-stats))

(def process-v5-validations
  (batch-process-fn psql/xml-tree-validations
                    psql/xml-tree-validation-value
                    tree-stats/store-tree-stats))
