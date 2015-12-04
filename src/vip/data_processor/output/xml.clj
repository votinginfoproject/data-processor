(ns vip.data-processor.output.xml
  "Generating the XML output for an import. XML generation is handled
  by clojure.xml, so XML elements must be represented by the kinds of
  maps it expects:

  {:tag :tag-name, :attrs {:map-of :attributes} :content [{:more :elements} ...]}

  The pipeline to add child elements to the main vip_object element is
  happens in the `write-xml` function itself. The `xml-node-fns` are
  used to generate the nodes. Those functions take a processing
  context and return a sequence of XML elements to add as children to
  the vip_object element.

  `write-xml` lazily writes the XML file, so processing does not need
  to take place all in memory."
  (:require [clojure.xml :as xml]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [vip.data-processor.output.v3-0.xml :as v3-0])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [javax.xml XMLConstants]
           [javax.xml.transform.stream StreamSource]
           [javax.xml.validation SchemaFactory]
           [org.xml.sax SAXParseException]
           [org.apache.commons.lang StringEscapeUtils]))

(defn create-xml-file [{:keys [filename] :as ctx}]
  (let [xml-file (Files/createTempFile filename ".xml" (into-array FileAttribute []))]
    (-> ctx
        (assoc :xml-output-file xml-file)
        (update :to-be-cleaned conj xml-file))))

(def ^:const SPACE " ")
(def ^:const OPEN-VALUE "=\"")
(def ^:const CLOSE-VALUE "\"")
(def ^:const OPEN-TAG "<")
(def ^:const CLOSE-TAG ">")
(def ^:const OPEN-END-TAG "</")

(defn emit-element [^java.io.Writer writer e]
  (if (string? e)
    (StringEscapeUtils/escapeXml writer e)
    (let [{:keys [tag attrs content]} e]
      (.write writer OPEN-TAG)
      (.write writer (name tag))
      (doseq [[k v] attrs]
        (.write writer SPACE)
        (.write writer (name k))
        (.write writer OPEN-VALUE)
        (.write writer (str v))
        (.write writer CLOSE-VALUE))
      (.write writer CLOSE-TAG)
      (doseq [c content]
        (emit-element writer c))
      (.write writer OPEN-END-TAG)
      (.write writer (name tag))
      (.write writer CLOSE-TAG))))

(defn-traced write-xml
  "Writes out the XML elements of nodes from `xml-node-fns` as
  children of the base `vip_object` element."
  [{:keys [xml-output-file vip-version] :as ctx}]
  ;; TODO: select `xml-node-fns` based on `vip-version`
  (let [xml-node-fns v3-0/xml-node-fns
        ;; TODO: select `vip-object` based on `vip-version`
        vip-object v3-0/vip-object]
    (with-open [out-file (io/writer (.toFile xml-output-file))]
      (let [[opening-tag closing-tag] (-> vip-object
                                          xml/emit-element
                                          with-out-str
                                          (string/split #"\n"))
            counter (atom 0)]
        (.write out-file "<?xml version='1.0' encoding='UTF-8'?>\n")
        (.write out-file opening-tag)
        (doseq [xml-node-fn xml-node-fns
                xml-child (xml-node-fn ctx)]
          (swap! counter inc)
          (when (zero? (mod @counter 1000))
            (log/info "Wrote" @counter "XML nodes"))
          (emit-element out-file xml-child))
        (.write out-file closing-tag))))
  ctx)

(defn validate-xml-output [{:keys [xml-output-file vip-version] :as ctx}]
  ;; TODO: choose `spec-resource` based on `vip-version`
  (let [spec-resource "specs/vip_spec_v3.0.xsd"
        schema-factory (SchemaFactory/newInstance XMLConstants/W3C_XML_SCHEMA_NS_URI)
        schema (.newSchema schema-factory (io/resource spec-resource))
        validator (.newValidator schema)]
    (try
      (.validate validator (StreamSource. (.toFile xml-output-file)))
      ctx
      (catch SAXParseException e
        (assoc-in ctx [:fatal :xml-generation :global :invalid-xml] [(.getMessage e)])))))

(def pipeline
  [create-xml-file
   write-xml
   validate-xml-output])
