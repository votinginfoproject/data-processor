(ns vip.data-processor.validation.xml.spec
  (:import [javax.xml.parsers DocumentBuilder DocumentBuilderFactory]
           [javax.xml.xpath XPathFactory XPathConstants]
           [javax.xml XMLConstants]
           [javax.xml.namespace NamespaceContext])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def ns-context
  "A NamespaceContext aware of the xs namespace."
  (reify NamespaceContext
    (getNamespaceURI [this prefix]
      (case prefix
        XMLConstants/DEFAULT_NS_PREFIX XMLConstants/NULL_NS_URI
        "xs" "http://www.w3.org/2001/XMLSchema"
        (throw (IllegalArgumentException. (str "Unknown prefix: " prefix)))))
    (getPrefix [this namespace-uri]
      (case namespace-uri
        XMLConstants/NULL_NS_URI XMLConstants/DEFAULT_NS_PREFIX
        XMLConstants/XML_NS_URI XMLConstants/XML_NS_PREFIX
        XMLConstants/XMLNS_ATTRIBUTE_NS_URI XMLConstants/XMLNS_ATTRIBUTE
        "http://www.w3.org/2001/XMLSchema" "xs"
        (throw (IllegalArgumentException. (str "Unknown namespace-uri: " namespace-uri)))))
    (getPrefixes [this namespace-uri]
      ;; not implemented
      nil)))

(defn spec-resource [version]
  (let [path (str "specs/vip_spec_v" version ".xsd")]
    (io/resource path)))

(def doc-builder-factory (doto (DocumentBuilderFactory/newInstance)
                           (.setNamespaceAware true)))
(def doc-builder (.newDocumentBuilder doc-builder-factory))

(defn parse-spec-by-version [version]
  (with-open [is (io/input-stream (spec-resource version))]
                (.parse doc-builder is)))

(def spec-docs
  (let [versions ["3.0" "5.1"]]
    (zipmap versions
            (map parse-spec-by-version versions))))

(def xpath-factory (XPathFactory/newInstance))

(defn query-spec
  "Given an XPath expression and a specification version returns all
  nodes that match."
  [path version]
  (let [xpath (doto (.newXPath xpath-factory)
                (.setNamespaceContext ns-context))
        expr (.compile xpath path)
        spec-doc (spec-docs version)]
    (.evaluate expr spec-doc XPathConstants/NODESET)))

(defn nodeset->seq* [nodeset length i]
  (if (< i length)
    (lazy-seq
     (cons (.item nodeset i)
           (nodeset->seq* nodeset length (inc i))))
    nil))

(defn nodeset->seq [nodeset]
  (nodeset->seq* nodeset (.getLength nodeset) 0))

(defn ancestry
  "Returns a list of all ancestors (except the 'document') for the
  element."
  [element]
  (loop [ancestors nil
         ancestor element]
    (if ancestor
      (recur (conj ancestors ancestor)
             (.getParentNode ancestor))
      (->> ancestors
           (drop 1) ;; drop the "document" element
           reverse))))

(defn query->elements [query version]
  (-> query
      (query-spec version)
      nodeset->seq))

(defn type->elements
  "Query a spec for all elements of the type."
  [type version]
  (let [type-query-expr (str "//*[@type='" type "']")
        extension-query-expr (str "//xs:extension[@base='" type "']")]
    (concat
     (query->elements type-query-expr version)
     (query->elements extension-query-expr version))))

(declare paths-for-type)

(defn paths-for-element [element version]
  (let [ancestors (ancestry element)]
    (reduce (fn [paths ancestor]
              (case (.getTagName ancestor)
                "xs:element" (map #(conj % (.getAttribute ancestor "name")) paths)
                "xs:complexType" (let [name (.getAttribute ancestor "name")]
                                   (if (= "" name)
                                     paths
                                     (let [type-paths (paths-for-type name version)]
                                       (map #(concat % paths) type-paths))))
                paths))
            '(()) ancestors)))

(defn paths-for-type [type version]
  (let [elements (type->elements type version)]
    (->> elements
         (mapcat #(paths-for-element % version))
         (map flatten))))

(defn type->simple-paths
  "Generates a list of simple-paths to find all elements of type in
  the version of the specification."
  [type version]
  {:pre [(contains? spec-docs version)]}
  (->> (paths-for-type type version)
       (map (partial str/join "."))))

(defn enumeration-values [type version]
  (let [query (str "//xs:simpleType[@name='" type "']//xs:enumeration")]
    (->> (query->elements query version)
         (map #(.getAttribute % "value"))
         set)))
