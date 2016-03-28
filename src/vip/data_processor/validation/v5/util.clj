(ns vip.data-processor.validation.v5.util
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.string :as str]))

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

(defn select-lquery
  "Returns the xml-tree-values whose results_id matches the given `import-id`
  and that match an ltree query on `path`. Returns a korma results collection.

  Example: (select-lquery 1 \"VipObject.0.Source.*{1}\") will run a query like:
           SELECT * FROM xml_tree_values WHERE results_id = 1
           AND path ~ 'VipObject.0.Source.*{1}'"
  [import-id path]
  (korma/select postgres/xml-tree-values
                (korma/where {:results_id import-id})
                (korma/where
                 (postgres/ltree-match
                  postgres/xml-tree-values :path path))))

(defn keyword->xml-name
  "Converts Clojure kebaab-case keywords into XML camel-case strings."
  [kw]
  (let [components (-> kw name (str/split #"-"))]
    (str/join (map str/capitalize components))))

(defn build-no-missing-validators
  "Returns a coll of fns that validates whether every element of `schema-type`
  has a child `element` in the import identified by `import-id`."
  [schema-type element import-id]
  (let [xml-schema-type (keyword->xml-name schema-type)
        paths (spec/type->simple-paths xml-schema-type "5.0")
        path-element (keyword->xml-name element)]
    (for [p paths]
      (let [simple-path-nlevel (-> p
                                   (str/split #"\.")
                                   count)
            path-nlevel (* 2 simple-path-nlevel)
            path2-nlevel (inc path-nlevel)]
        (build-xml-tree-value-query-validator
         :errors schema-type :missing (->> element
                                           name
                                           (str "missing-")
                                           keyword)
         "SELECT xtv.path
          FROM (SELECT DISTINCT subltree(path, 0, CAST(? AS INT)) || ? AS path
                FROM xml_tree_values WHERE results_id = ?
                AND subltree(simple_path, 0, CAST(? AS INT)) = text2ltree(?)) xtv
          LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
          ON xtv.path = subltree(xtv2.path, 0, CAST(? AS INT))
          WHERE xtv2.path IS NULL"
         (constantly [path-nlevel path-element import-id simple-path-nlevel p
                      import-id path2-nlevel]))))))

(defn validate-no-missing-elements
  "Returns a fn that takes a validation `ctx` and runs 'no-missing' validators
  on all elements of `schema-type` in that import, checking that they all have
  `element` children."
  [schema-type element]
  (fn [{:keys [import-id] :as ctx}]
    (let [validators (build-no-missing-validators schema-type
                                                  element
                                                  import-id)]
      (reduce (fn [ctx validator] (validator ctx)) ctx validators))))
