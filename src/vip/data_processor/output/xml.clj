(ns vip.data-processor.output.xml
  "Generating the XML output for an import. XML generation is handled
  by clojure.xml, so XML elements must be represented by the kinds of
  maps it expects:

  {:tag :tag-name, :attrs {:map-of :attributes} :content [{:more :elements} ...]}

  The pipeline to add child elements to the main vip_object element is
  mostly governed by the `add-xml-children` function. Functions passed
  to `add-xml-children` take a processing context and return a
  sequence of XML elements to add as children to the vip_object
  element.

  `write-xml` lazily writes the XML file, so processing does not need
  to take place all in memory."
  (:require [clojure.xml :as xml]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [vip.data-processor.output.ballot :as ballot]
            [vip.data-processor.output.ballot-line-result :as ballot-line-result]
            [vip.data-processor.output.ballot-response :as ballot-response]
            [vip.data-processor.output.candidate :as candidate]
            [vip.data-processor.output.contest :as contest]
            [vip.data-processor.output.early-vote-site :as early-vote-site]
            [vip.data-processor.output.election :as election]
            [vip.data-processor.output.election-official :as election-official]
            [vip.data-processor.output.electoral-district :as electoral-district]
            [vip.data-processor.output.polling-location :as polling-location]
            [vip.data-processor.output.precinct :as precinct]
            [vip.data-processor.output.referendum :as referendum]
            [vip.data-processor.output.source :as source]
            [vip.data-processor.output.state :as state]
            [vip.data-processor.output.street-segment :as street-segment])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.apache.commons.lang StringEscapeUtils]))

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

(defn- escape-content
  "For use in walking xml element maps."
  [form]
  (if (and (vector? form)
           (= :content (first form))
           (every? string? (second form)))
    [:content (map #(StringEscapeUtils/escapeXml %) (second form))]
    form))

(defn write-xml
  "Writes out the XML elements of `xml-children` as children of the
  base `vip_object` element. Uses `do-seq` so that `xml-children` can
  be lazy and larger than memory."
  [{:keys [xml-output-file xml-children] :as ctx}]
  (with-open [out-file (io/writer (.toFile xml-output-file))]
    (let [[opening-tag closing-tag] (-> vip-object
                                        xml/emit-element
                                        with-out-str
                                        (string/split #"\n"))]
      (.write out-file "<?xml version='1.0' encoding='UTF-8'?>\n")
      (.write out-file opening-tag)
      (doseq [xml-child (:xml-children ctx)]
        (let [xml-child (walk/postwalk escape-content xml-child)
              xml-str (-> xml-child
                          xml/emit-element
                          with-out-str
                          (string/replace #"(>)\n|\n(<)" "$1$2"))]
          (.write out-file xml-str)))
      (.write out-file closing-tag)))
  ctx)

(defn add-xml-children
  "Takes a function that takes a proecssing context and returns
  top-level clojure.xml-style maps . Returns a processing function
  that calls the passed function and lazily adds the returned maps to
  the :xml-children key of the context."
  [xml-fn]
  (fn [ctx]
    (let [children (xml-fn ctx)]
      (update ctx :xml-children #(lazy-cat % %2) children))))

(def pipeline
  [create-xml-file
   (add-xml-children ballot/xml-nodes)
   (add-xml-children ballot-line-result/xml-nodes)
   (add-xml-children ballot-response/xml-nodes)
   (add-xml-children candidate/xml-nodes)
   (add-xml-children contest/xml-nodes)
   (add-xml-children early-vote-site/xml-nodes)
   (add-xml-children election/xml-nodes)
   (add-xml-children election-official/xml-nodes)
   (add-xml-children electoral-district/xml-nodes)
   (add-xml-children polling-location/xml-nodes)
   (add-xml-children precinct/xml-nodes)
   (add-xml-children referendum/xml-nodes)
   (add-xml-children source/xml-nodes)
   (add-xml-children state/xml-nodes)
   (add-xml-children street-segment/xml-nodes)
   write-xml])
