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
            [vip.data-processor.output.ballot :as ballot]
            [vip.data-processor.output.ballot-line-result :as ballot-line-result]
            [vip.data-processor.output.ballot-response :as ballot-response]
            [vip.data-processor.output.candidate :as candidate]
            [vip.data-processor.output.contest :as contest]
            [vip.data-processor.output.contest-result :as contest-result]
            [vip.data-processor.output.custom-ballot :as custom-ballot]
            [vip.data-processor.output.early-vote-site :as early-vote-site]
            [vip.data-processor.output.election :as election]
            [vip.data-processor.output.election-administration :as election-administration]
            [vip.data-processor.output.election-official :as election-official]
            [vip.data-processor.output.electoral-district :as electoral-district]
            [vip.data-processor.output.locality :as locality]
            [vip.data-processor.output.polling-location :as polling-location]
            [vip.data-processor.output.precinct :as precinct]
            [vip.data-processor.output.precinct-split :as precinct-split]
            [vip.data-processor.output.referendum :as referendum]
            [vip.data-processor.output.source :as source]
            [vip.data-processor.output.state :as state]
            [vip.data-processor.output.street-segment :as street-segment])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [javax.xml XMLConstants]
           [javax.xml.transform.stream StreamSource]
           [javax.xml.validation SchemaFactory]
           [org.xml.sax SAXParseException]
           [org.apache.commons.lang StringEscapeUtils]))

(def xml-node-fns [ballot/xml-nodes
                   ballot-line-result/xml-nodes
                   ballot-response/xml-nodes
                   candidate/xml-nodes
                   contest/xml-nodes
                   contest-result/xml-nodes
                   custom-ballot/xml-nodes
                   early-vote-site/xml-nodes
                   election/xml-nodes
                   election-administration/xml-nodes
                   election-official/xml-nodes
                   electoral-district/xml-nodes
                   locality/xml-nodes
                   polling-location/xml-nodes
                   precinct/xml-nodes
                   precinct-split/xml-nodes
                   referendum/xml-nodes
                   source/xml-nodes
                   state/xml-nodes
                   street-segment/xml-nodes])

(def vip-object-attrs
  {:xmlns:xsi "http://www.w3.org/2001/XMLSchema-instance"
   :xsi:noNamespaceSchemaLocation "http://election-info-standard.googlecode.com/files/election_spec_v3.0.xsd"
   :schemaVersion "3.0"})

(def vip-object
  {:tag :vip_object
   :attrs vip-object-attrs
   :content []})

(defn create-xml-file [{:keys [filename] :as ctx}]
  (assoc ctx
         :xml-output-file
         (Files/createTempFile filename ".xml" (into-array FileAttribute []))))

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

(defn write-xml
  "Writes out the XML elements of nodes from `xml-node-fns` as
  children of the base `vip_object` element."
  [{:keys [xml-output-file xml-children] :as ctx}]
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
      (.write out-file closing-tag)))
  ctx)

(defn validate-xml-output [{:keys [xml-output-file] :as ctx}]
  (let [schema-factory (SchemaFactory/newInstance XMLConstants/W3C_XML_SCHEMA_NS_URI)
        schema (.newSchema schema-factory (io/resource "specs/vip_spec_v3.0.xsd"))
        validator (.newValidator schema)]
    (try
      (.validate validator (StreamSource. (.toFile xml-output-file)))
      ctx
      (catch SAXParseException e
        (assoc-in ctx [:fatal :xml-generation :validation] (.getMessage e))))))

(def pipeline
  [create-xml-file
   write-xml
   validate-xml-output])
