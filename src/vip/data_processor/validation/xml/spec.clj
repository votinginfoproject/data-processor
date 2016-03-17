(ns vip.data-processor.validation.xml.spec
  (:import [javax.xml.parsers DocumentBuilder DocumentBuilderFactory]
           [javax.xml.xpath XPathFactory XPathConstants])
  (:require [clojure.java.io :as io]))

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
  (let [versions ["3.0" "5.0"]]
    (zipmap versions
            (map parse-spec-by-version versions))))

(def xpath-factory (XPathFactory/newInstance))

(defn query-spec
  "Given an XPath expression and a specification version returns all
  nodes that match."
  [path version]
  (let [xpath (.newXPath xpath-factory)
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
      ;; drop the "document" element
      (drop 1 ancestors))))

(defn type->elements
  "Query a spec for all elements of the type."
  [type version]
  (let [type-query-expr (str "//*[@type='" type "']")]
    (-> type-query-expr
        (query-spec version)
        nodeset->seq)))

(declare paths-for-type)

(defn paths-for-element [element version]
  (let [ancestors (ancestry element)]
    (reduce (fn [paths ancestor]
              (case (.getTagName ancestor)
                "xs:element" (conj paths (.getAttribute ancestor "name"))
                "xs:complexType" (if-let [type-paths (seq (paths-for-type (.getAttribute ancestor "name") version))]
                                   (conj paths type-paths)
                                   paths)
                paths))
            nil ancestors)))

(defn paths-for-type [type version]
  (let [elements (type->elements type version)]
    (map #(paths-for-element % version) elements)))

(defn explode-paths [paths]
  (loop [lqueries [[]]
         [x & xs] paths]
    (cond
      (nil? x) (map flatten lqueries)
      (string? x) (recur (map #(conj % x) lqueries)
                         xs)
      :else (let [forked-paths (map explode-paths x)]
              (recur (map #(concat lqueries %) forked-paths)
                     xs)))))

(defn path->lquery [path]
  (->> path
       (interleave (repeat "*{1}"))
       (interpose ".")
       reverse
       (apply str)))

(defn type->lqueries
  "Generates a list of lqueries to find all elements of type in the
  version of the specification."
  [type version]
  {:pre [(contains? spec-docs version)]}
  (->> (paths-for-type type version)
       (mapcat explode-paths)
       (map path->lquery)))
