(ns vip.data-processor.validation.xml
  (:require [clojure.data.xml :as xml]
            [clojure.walk :refer [stringify-keys]]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]))

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

(defn determine-spec-version [ctx]
  (let [xml-file (first (:input ctx))]
    (with-open [reader (util/bom-safe-reader xml-file)]
      (let [vip-object (xml/parse reader)
            version (get-in vip-object [:attrs :schemaVersion])]
        (assoc ctx :xml-version version)))))

(defn unsupported-version [{:keys [xml-version] :as ctx}]
  (assoc ctx :stop (str "Unsupported XML version: " xml-version)))

(def version-pipelines
  {"3.0" [load-xml]
   "5.0" [unsupported-version]})

(defn branch-on-spec-version [{:keys [xml-version] :as ctx}]
  (if-let [pipeline (get version-pipelines xml-version)]
    (update ctx :pipeline (partial concat pipeline))
    (unsupported-version ctx)))
