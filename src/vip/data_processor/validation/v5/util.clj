(ns vip.data-processor.validation.v5.util
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

(defn two-import-ids
  "A common params-fn for build-xml-tree-value-query-validator that
  returns a 2-element vector of the import-id of the context twice."
  [{:keys [import-id]}]
  [import-id import-id])

(defn build-xml-tree-value-query-validator
  "Generate a validator that adds a validation error for every path in
  the results of the query. The params-fn must be a fn of one argument (the
  context), which reutrns a vector of params for the query."
  [severity scope error-type error-data query params-fn]
  (fn [{:keys [import-id] :as ctx}]
    (let [missing-paths (korma/exec-raw
                         (:conn postgres/xml-tree-values)
                         [query (params-fn ctx)]
                         :results)]
      (->> missing-paths
           (map :path)
           (reduce (fn [ctx path]
                     (update-in ctx [severity scope (.getValue path) error-type]
                                conj error-data))
                   ctx)))))
