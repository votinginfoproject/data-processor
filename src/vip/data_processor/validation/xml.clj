(ns vip.data-processor.validation.xml
  (:require [clojure.data.xml :as xml]
            [clojure.walk :refer [stringify-keys]]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.v5 :as v5-validations]
            [vip.data-processor.validation.xml.v5 :as xml.v5]))

(def address-elements
  #{"address"
    "physical_address"
    "mailing_address"
    "filed_mailing_address"
    "non_house_address"})

(defn suffix-map [suffix map]
  (reduce-kv (fn [suffixed-map k v]
               (assoc suffixed-map
                      (str suffix "_" k)
                      v))
             {}
             map))

(defn flatten-address-elements [map]
  (reduce (fn [element address-key]
            (if-let [address (element address-key)]
              (merge (dissoc element address-key)
                     (suffix-map address-key address))
              element))
          map
          address-elements))

(declare element->map)

(defn node->key-value [{:keys [tag content] :as node}]
  (let [tag (name tag)]
    (cond
      (empty? content)
      [tag nil]

      (every? (partial instance? clojure.data.xml.Element) content)
      [tag (element->map node)]

      :else
      [tag (first content)])))

(defn element->map [{:keys [attrs content]}]
  (-> attrs
      stringify-keys
      (into (map node->key-value content))
      flatten-address-elements))

(defn-traced validate-format-rules [ctx rows {:keys [table columns]}]
  (let [format-rules (data-spec/create-format-rules (:data-specs ctx) table columns)]
    (reduce (fn [ctx row]
              (data-spec/apply-format-rules format-rules ctx row (row "id")))
            ctx rows)))

(defn is-tag? [elem tag]
  (= (keyword tag) (:tag elem)))

(defn element->joins [id-name joined-id elem]
  (let [id (:id (:attrs elem))
        join-elements (filter #(is-tag? % joined-id) (:content elem))]
    (map (fn [join-elem] {id-name id joined-id (first (:content join-elem))})
         join-elements)))

(defn-traced import-joins [ctx {:keys [xml-references] :as data-spec} elements]
  (reduce (fn [ctx {:keys [join-table id joined-id]}]
            (let [sql-table (get-in ctx [:tables join-table])
                  join-contents (mapcat (partial element->joins id joined-id) elements)]
              (sqlite/bulk-import ctx sql-table join-contents)))
          ctx
          xml-references))

(defn-traced load-elements [ctx elements]
  (let [tag (:tag (first elements))]
    (if-let [data-spec (first (filter #(= tag (:tag-name %)) (:data-specs ctx)))]
      (let [element-maps (map element->map elements)
            table-key (:table data-spec)
            sql-table (get-in ctx [:tables table-key])
            ctx (validate-format-rules ctx element-maps data-spec)
            columns (:columns data-spec)
            column-names (map :name columns)
            contents (map #(select-keys % column-names) element-maps)
            transforms (apply comp (data-spec/translation-fns columns))
            transformed-contents (map transforms contents)]
        (import-joins ctx data-spec elements)
        (sqlite/bulk-import ctx sql-table transformed-contents))
      (update-in ctx [:critical :import :global :unknown-tags] conj tag))))

(defn partition-by-n
  "Applies f to each value in coll, splitting it each time f returns a
  new value up to a maximum of n elements.  Returns a lazy seq of
  partitions."
  [f n coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [fst (first s)
           fv (f fst)
           next-n (take (dec n) (next s))
           run (cons fst (take-while #(= fv (f %)) next-n))]
       (cons run (partition-by-n f n (seq (drop (count run) s))))))))

(defn-traced load-xml
  "Load the XML input file into the database, validating as we go.

  Since XML is parsed as a lazy stream, reading off elements may
  eventually blow up. If `reduce` were used on that stream, we would
  lose any work completed up to that point. By explicitly looping, we
  may catch the exception and continue with further validations of
  what has been captured."
  [ctx]
  (let [xml-file (first (:input ctx))
        reader (util/bom-safe-reader xml-file)
        partitioned-xml-elements (partition-by-n :tag 5000 (:content (xml/parse reader)))]
    (loop [ctx ctx
           xml-elements partitioned-xml-elements]
      (let [partition-or-error (try
                                 (first xml-elements)
                                 (catch javax.xml.stream.XMLStreamException e
                                   e))]
        (cond
          ;; If there was nothing to take, we're done
          (nil? partition-or-error) ctx

          ;; If we get a XMLStreamException, add error and finish
          (instance? javax.xml.stream.XMLStreamException partition-or-error)
          (assoc-in ctx [:critical :import :global :malformed-xml]
                    [(.getMessage partition-or-error)])

          ;; otherwise, load and continue
          :else (recur (load-elements ctx partition-or-error) (rest xml-elements)))))))

(defn simple-value?
  "In the context of dealing with xml.Elements, anything not an
  xml.Element is a simple value."
  [x]
  (not (instance? clojure.data.xml.Element x)))

(defn ltree-path-join
  "Join ltree path parts together, rejecing nils, and using the name
  of a piece if it is named (so that path parts don't include colons
  from keywords, for example).

  (ltree-path-join \"hello.1\" :hi) ;;=> \"hello.1.hi\""
  [& parts]
  (->> parts
       (keep identity)
       (map (fn [x]
              (if (instance? clojure.lang.Named x)
                (name x)
                x)))
       (interpose ".")
       (apply str)))

(defn path-and-values
  "Lazily produces a sequence of maps of path, value, and
  parent_with_id keys from an clojure.data.xml.Element. A map is
  produced for each attribute and each element with a simple
  value. For example, an XML document like:

  <foo id=\"foo1\">
    <bar>3</bar>
    <baz id=\"baz1\">
      <quux>hello</quux>
    </baz>
    <test>yes</test>
  </foo>

  Would produce the following:

  ({:path \"foo.0.id\", :value \"foo1\", :parent_with_id \"foo.0\"}
   {:path \"foo.0.bar.0\", :value \"3\", :parent_with_id \"foo.0\"}
   {:path \"foo.0.baz.1.id\", :value \"baz1\", :parent_with_id \"foo.0.baz.1\"}
   {:path \"foo.0.baz.1.quux.0\", :value \"hello\", :parent_with_id \"foo.0.baz.1\"}
   {:path \"foo.0.test.2\", :value \"yes\", :parent_with_id \"foo.0\"})

  The paths generated include a sequence order for
  elements. `foo.0.baz.1.quux.0` refers to the `quux` element which is
  the first child of the `baz` element which is the second child of
  the `foo` element, which is the first element in the document."
  ([node]
   (path-and-values node nil nil 0 nil))
  ([node current-path current-simple-path n nearest-path-with-id]
   (let [element-path (ltree-path-join current-path (:tag node) n)
         simple-path (ltree-path-join current-simple-path (:tag node))
         nearest-path-with-id (if (get-in node [:attrs :id])
                                element-path
                                nearest-path-with-id)
         attribute-entries (map (fn [[k v]]
                                  {:path (ltree-path-join element-path k)
                                   :simple_path (ltree-path-join simple-path k)
                                   :value v
                                   :parent_with_id nearest-path-with-id})
                                (:attrs node))
         child-entries (map-indexed
                        (fn [n value]
                          (if (simple-value? value)
                            [{:path element-path
                              :simple_path simple-path
                              :value value
                              :parent_with_id nearest-path-with-id}]
                            (path-and-values value element-path simple-path
                                             n nearest-path-with-id)))
                        (:content node))]
     (apply concat attribute-entries child-entries))))

(defn load-xml-ltree
  [ctx]
  (let [xml-file (first (:input ctx))
        import-id (:import-id ctx)]
    (with-open [reader (util/bom-safe-reader xml-file)]
      (doseq [pvs (->> reader
                       xml/parse
                       path-and-values
                       (partition 5000 5000 '())
                       (map (fn [chunk]
                              (map (fn [path-map]
                                     (-> path-map
                                         (update :path postgres/path->ltree)
                                         (update :parent_with_id postgres/path->ltree)
                                         (update :simple_path postgres/path->ltree)
                                         (assoc :results_id import-id)))
                                   chunk))))]
        (korma/insert postgres/xml-tree-values
          (korma/values pvs))))
    ctx))

(defn load-xml-tree-validations
  [ctx]
  (let [results-id (:import-id ctx)
        errors (postgres/xml-tree-validation-values ctx)]
    (postgres/bulk-import ctx
                          postgres/xml-tree-validations
                          errors)))

(defn determine-spec-version [ctx]
  (let [xml-file (first (:input ctx))]
    (with-open [reader (util/bom-safe-reader xml-file)]
      (let [vip-object (xml/parse reader)
            version (get-in vip-object [:attrs :schemaVersion])]
        (assoc ctx :spec-version version)))))

(defn unsupported-version [{:keys [spec-version] :as ctx}]
  (assoc ctx :stop (str "Unsupported XML version: " spec-version)))

(defn set-input-as-xml-output-file
  [{:keys [input] :as ctx}]
  (assoc ctx :xml-output-file
         (first input)))

(def version-pipelines
  {"3.0" [sqlite/attach-sqlite-db
          load-xml]
   "5.1" (concat [load-xml-ltree
                  xml.v5/load-xml-street-segments]
                 v5-validations/validations
                 [load-xml-tree-validations
                  set-input-as-xml-output-file])})

(defn branch-on-spec-version [{:keys [spec-version] :as ctx}]
  (if-let [pipeline (get version-pipelines spec-version)]
    (update ctx :pipeline (partial concat pipeline))
    (unsupported-version ctx)))
