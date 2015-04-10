(ns vip.data-processor.validation.xml
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.walk :refer [stringify-keys]]
            [vip.data-processor.db.sqlite :as sqlite]
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

(defn validate-format-rules [ctx rows {:keys [tag-name columns]}]
  (let [format-rules (data-spec/create-format-rules tag-name columns)]
    (reduce (fn [ctx row]
              (data-spec/apply-format-rules format-rules ctx row (:id row)))
            ctx rows)))

(defn load-elements [ctx elements]
  (let [tag (:tag (first elements))]
    (if-let [data-spec (first (filter #(= tag (:tag-name %)) data-spec/data-specs))]
      (let [element-maps (map element->map elements)
            table-key (:table data-spec)
            sql-table (get-in ctx [:tables table-key])
            ctx (validate-format-rules ctx element-maps data-spec)
            columns (:columns data-spec)
            column-names (map :name columns)
            contents (map #(select-keys % column-names) element-maps)
            transforms (apply comp (data-spec/translation-fns columns))
            transformed-contents (map transforms contents)]
        (sqlite/bulk-import transformed-contents sql-table)
        ctx)
      (assoc-in ctx [:critical :xml-import tag :unknown] tag))))

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

(defn load-xml [ctx]
  (let [xml-file (first (:input ctx))]
    (reduce load-elements ctx (partition-by-n :tag 100 (:content (xml/parse (io/reader xml-file)))))))
