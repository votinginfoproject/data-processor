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
  context) which returns a vector of params for the query."
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

(defn select-path
  "Returns the xml-tree-values whose results_id matches the given `import-id`
  and that match an ltree query on `path`. Returns a korma results collection.

  Example: (select-path 1 \"VipObject.0.Source.id\") will run a query like:
           SELECT * FROM xml_tree_values WHERE results_id = 1
           AND path ~ 'VipObject.0.Source.id'"
  [import-id path]
  (korma/select postgres/xml-tree-values
                (korma/where {:results_id import-id})
                (korma/where
                 (postgres/ltree-match
                  postgres/xml-tree-values :path path))))
