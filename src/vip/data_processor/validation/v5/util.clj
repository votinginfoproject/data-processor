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
  "Converts Clojure kebab-case keywords into XML camel-case strings."
  [kw]
  (let [components (-> kw name (str/split #"-"))]
    (str/join (map str/capitalize components))))

(defn xml-name->keyword
  "Converts XML camel-case strings into Clojure kebab-case keywords."
  [xml-name]
  (->> xml-name
       (re-seq #"[A-Z][a-z0-9]*")
       (map str/lower-case)
       (str/join "-")
       keyword))

(defn build-no-missing-validators
  "Returns a coll of fns that validates whether every element of `schema-type`
  has a child at `element-path` (a vec of kebaab-case element names) in the
  import identified by `import-id`."
  [schema-type element-path import-id]
  (let [xml-schema-type (keyword->xml-name schema-type)
        paths (spec/type->simple-paths xml-schema-type "5.0")
        xml-element-path (mapv keyword->xml-name element-path)]
    (for [p paths]
      (let [path-to-parent-components (-> p
                                          (str/split #"\.")
                                          (concat (butlast xml-element-path)))
            path-to-parent (str/join "." path-to-parent-components)
            simple-path-nlevel (count path-to-parent-components)
            path-nlevel (* 2 simple-path-nlevel)
            path2-nlevel (inc path-nlevel)]
        (build-xml-tree-value-query-validator
         :errors schema-type :missing (->> element-path
                                           (map name)
                                           (str/join "-")
                                           (str "missing-")
                                           keyword)
         "SELECT xtv.path
          FROM (SELECT DISTINCT subltree(path, 0, CAST(? AS INT)) || ? AS path
                FROM xml_tree_values WHERE results_id = ?
                AND subltree(simple_path, 0, CAST(? AS INT)) = text2ltree(?)) xtv
          LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
          ON xtv.path = subltree(xtv2.path, 0, CAST(? AS INT))
          WHERE xtv2.path IS NULL"
         (constantly [path-nlevel (last xml-element-path) import-id
                      simple-path-nlevel path-to-parent import-id
                      path2-nlevel]))))))

(defn validate-no-missing-elements
  "Returns a fn that takes a validation `ctx` and runs 'no-missing' validators
  on all elements of `schema-type` in the `ctx`'s import, checking that they
  all have `element` children."
  [schema-type element-path]
  (fn [{:keys [import-id] :as ctx}]
    (let [validators (build-no-missing-validators schema-type
                                                  element-path
                                                  import-id)]
      (reduce (fn [ctx validator] (validator ctx)) ctx validators))))

(defn elements-for-simple-path
  "Returns all elements in `import-id` whose `simple-path` matches the given
  arg."
  [import-id simple-path]
  (korma/select postgres/xml-tree-values
    (korma/where
     {:results_id import-id
      :simple_path (postgres/path->ltree simple-path)})))

(defn nth-to-last-path-element
  "Returns the ltree path element `n` segments from the end."
  [path n]
  (-> path
      (str/split #"\.")
      reverse
      (nth n)))

(defn validate-elements
  "Returns a fn that takes a validation `ctx` and runs the supplied `valid?`
  predicate fn on every element of `schema-type` in the `ctx`'s import."
  [schema-type valid?]
  (fn [ctx]
    (let [xml-schema-type (keyword->xml-name schema-type)
          validators (for [p (spec/type->simple-paths xml-schema-type "5.0")]
                       (let [parent-element (-> p
                                                (nth-to-last-path-element 1)
                                                xml-name->keyword)]
                         (fn [{:keys [import-id] :as ctx}]
                           (let [elements (elements-for-simple-path import-id p)
                                 invalid-elements (remove (comp valid? :value)
                                                          elements)]
                             (reduce (fn [ctx row]
                                       (update-in
                                        ctx
                                        [:errors parent-element
                                         (-> row :path .getValue) :format]
                                        conj (:value row)))
                                     ctx invalid-elements)))))]
      (reduce (fn [ctx validator] (validator ctx)) ctx validators))))

(defn validate-enum-elements
  "Returns a fn that takes a validation context and runs an enumerated values
  validation on all elements of `schema-type` in the context's import, checking
  that all of their values are one of those enumerated as valid by the schema.

  See `validate-elements` for more details."
  [schema-type]
  (let [xml-schema-type (keyword->xml-name schema-type)
        valid-values (spec/enumeration-values xml-schema-type "5.0")
        valid? #(valid-values %)]
    (validate-elements schema-type valid?)))
