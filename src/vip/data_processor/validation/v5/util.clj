(ns vip.data-processor.validation.v5.util
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.set :as set]))

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
    (log/info "Validating" severity scope error-type)
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
        paths (spec/type->simple-paths xml-schema-type "5.1")
        xml-element-path (mapv keyword->xml-name element-path)]
    (for [p paths]
      (let [path-to-parent-components (-> p
                                          (str/split #"\.")
                                          (concat (butlast xml-element-path))
                                          vec)
            path-to-parent (str/join "." path-to-parent-components)
            path-to-parent-nlevel (* 2 (count path-to-parent-components))
            path-to-children (-> path-to-parent-components
                                 (conj (last xml-element-path))
                                 (->> (str/join ".")))]
        (fn [ctx]
          (log/info "Validating" :errors schema-type :missing)
          (let [expected (->> (korma/exec-raw
                               (:conn postgres/xml-tree-values)
                               ["SELECT DISTINCT subltree(path, 0, CAST(? AS INT)) AS path
                                 FROM xml_tree_values
                                 WHERE results_id = ?
                                 AND simple_path <@ text2ltree(?)"
                                [path-to-parent-nlevel import-id
                                 path-to-parent]]
                               :results)
                              (map (comp #(.getValue %) :path))
                              set)
                found (->> (korma/exec-raw
                            (:conn postgres/xml-tree-values)
                            ["SELECT subltree(path, 0, CAST(? AS INT)) AS path
                              FROM xml_tree_values
                              WHERE results_id = ?
                              AND simple_path <@ text2ltree(?)"
                             [path-to-parent-nlevel import-id
                              path-to-children]]
                            :results)
                           (map (comp #(.getValue %) :path))
                           set)]
            (if (= expected found)
              ctx
              (let [missing (set/difference expected found)]
                (reduce (fn [ctx path]
                          (let [missing-path (str path "."
                                                  (last xml-element-path))]
                            (update-in ctx [:errors schema-type missing-path
                                            :missing]
                                       conj (->> element-path
                                                 (map name)
                                                 (str/join "-")
                                                 (str "missing-")
                                                 keyword))))
                        ctx missing)))))))))

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

(defmacro validate-no-missing-values
  "Create a bunch of validate-no-missing validators at once.

   `(validate-no-missing-values :street-segments [:city] [:state])`
  defs both validate-no-missing-city and validate-no-missing-state."
  [element & element-paths]
  (let [defs (map
              (fn [element-path]
                (let [validation-name
                      (symbol (str
                               "validate-no-missing-"
                               (str/join "-" (map name element-path))))]
                  `(def ~validation-name
                     (validate-no-missing-elements ~element
                                                   ~element-path))))
              element-paths)]
    `(do ~@defs)))

(defn elements-at-simple-path
  "Returns all elements in `import-id` whose `simple-path` matches the given
  arg."
  [import-id simple-path]
  (korma/select postgres/xml-tree-values
    (korma/where
     {:results_id import-id
      :simple_path (postgres/path->ltree simple-path)})))

(defn nth-to-last
  "Returns the element `n` segments from the end of `coll`."
  [coll n]
  (-> coll reverse (nth n)))

(defn error-root
  "Returns the appropriate keyword to use as the error root in the error message
  for an error at `simple-path`. It usually uses the penultimate path segment
  for this, but it will use the last one when the penultimate is 'VipObject'.

  For example, if `simple-path` is VipObject.FooBar.BazQux.Quux then error-root
  will be :baz-qux. If it is VipObject.FooBar then error-root will be :foo-bar."
  [simple-path]
  (let [path-components (str/split simple-path #"\.")
        second-to-last (nth-to-last path-components 1)]
    (xml-name->keyword (if (= second-to-last "VipObject")
                         (last path-components)
                         second-to-last))))

(defn element-validators
  "Returns the validation functions for all elements of `xml-schema-type` (a
  string representing an XML schema type name) at `xml-element-path` (a vector
  of strings representing XML element names) underneath those, using the `valid?`
  predicate fn to validate the text node of each element found at that path in
  the import context. When an error is found, it will be put into the context at
  `error-severity` level with `error-type` as its key.

  Primarily intended for use by `validate-elements`."
  [xml-schema-type xml-element-path valid? error-severity error-type]
  (for [p (spec/type->simple-paths xml-schema-type "5.1")]
    (fn [{:keys [import-id] :as ctx}]
      (let [simple-path (if (seq xml-element-path)
                          (str p "." (str/join "." xml-element-path))
                          p)
            error-root-element (error-root simple-path)
            elements (elements-at-simple-path import-id simple-path)
            invalid-elements (remove (comp valid? :value) elements)]
        (reduce (fn [ctx row]
                  (update-in ctx [error-severity error-root-element
                                  (-> row :path .getValue) error-type]
                             conj (:value row)))
                ctx invalid-elements)))))

(defn validate-elements
  "Returns a fn that takes a validation `ctx` and runs the supplied `valid?`
  predicate fn on every element of `schema-type` (optionally child elements
  below them defined by `:element-path`) in the `ctx`'s import. It will add
  errors to the context for any that `valid?` returns a falsey value at the
  `error-severity` level with an `error-type` key."
  [schema-type element-path valid? error-severity error-type]
  (fn [ctx]
    (let [xml-schema-type (keyword->xml-name schema-type)
          xml-element-path (mapv keyword->xml-name element-path)
          validators (element-validators xml-schema-type xml-element-path valid?
                                         error-severity error-type)]
      (reduce (fn [ctx validator] (validator ctx)) ctx validators))))

(defn validate-enum-elements
  "Returns a fn that takes a validation context and runs an enumerated values
  validation on all elements of `schema-type` in the context's import, checking
  that all of their values are one of those enumerated as valid by the schema.
  It will add errors to the context for any that `valid?` returns a falsey value
  at the `error-severity` level.

  See `validate-elements` for more details."
  [schema-type error-severity]
  (let [xml-schema-type (keyword->xml-name schema-type)
        valid? (spec/enumeration-values xml-schema-type "5.1")]
    (validate-elements schema-type [] valid? error-severity :format)))
