(ns vip.data-processor.output.tree-xml
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [korma.core :as korma]
            [vip.data-processor.output.xml-helpers :refer [create-xml-file]]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.util :as db.util]
            [clojure.tools.logging :as log]
            [vip.data-processor.db.postgres :as psql])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.apache.commons.lang StringEscapeUtils]))

(defn attr
  "Return the attribute of a path, if there is one."
  [path]
  (->> path
       (re-find #"(?:\.)(\D+)\z")
       second))

(defn opening-tag
  [tag]
  (str "<" tag ">"))

(defn closing-tag
  [tag]
  (str "</" tag ">"))

(defn tags-with-indexes
  "Returns a seq of the tags, with their indexes, of a path. Does not
  include attributes.

  (tags-with-indexes \"Root.0.Tag.10.id\") ;=> (\"Root.0\" \"Tag.10\")"
  [path]
  (when path
    (re-seq #"\w+\.\d+" path)))

(defn same-tag?
  "Are the two paths about the same tag. Thus:

  (same-tag? \"Root.0.Tag.10.id\" \"Root.0.Tag.10\") ;=> true"
  [prev-path path]
  (= (tags-with-indexes prev-path)
     (tags-with-indexes path)))

(defn shared-prefix
  "Returns a seq of the shared initial elements of two sequences."
  [a b]
  (loop [a a
         b b
         result []]
    (if (or (empty? a)
            (empty? b))
      result
      (let [[a1 & a-rest] a
            [b1 & b-rest] b]
        (if (= a1 b1)
          (recur a-rest
                 b-rest
                 (conj result a1))
          result)))))

(defn tag-without-index
  "Given a tag with an index, return the tag without the index."
  [tag-with-index]
  (re-find #"\A[^.]*" tag-with-index))

(defn to-close-and-to-open
  "Given a previous path and a current path, return the tags needed to
  be closed and the tags needed to be opened to transition from one to
  the other."
  [prev-path path]
  (let [prev-parts (tags-with-indexes prev-path)
        next-parts (tags-with-indexes path)
        shared-prefix (shared-prefix prev-parts next-parts)]
    {:to-close (->> prev-parts
                    (drop (count shared-prefix))
                    (map tag-without-index)
                    reverse)
     :to-open (->> next-parts
                   (drop (count shared-prefix))
                   (map tag-without-index))}))

(defn map-str [f]
  (fn [coll]
    (->> coll
         (map f)
         (apply str))))

(def opening-tags (map-str opening-tag))
(def closing-tags (map-str closing-tag))

(defn write-xml [file spec-version import-id]
  (with-open [f (io/writer (.toFile file))]
    (.write f (str "<?xml version=\"1.0\"?>\n<VipObject xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" schemaVersion=\"" spec-version "\" xsi:noNamespaceSchemaLocation=\"https://raw.githubusercontent.com/votinginfoproject/vip-specification/v" spec-version "-release/vip_spec.xsd\">\n"))
    (let [last-seen-path (atom "VipObject.0")
          inside-open-tag (atom false)
          values-written (atom 0)]
      (doseq [{:keys [path value simple_path] :as row}
              (psql/lazy-select-xml-tree-values 100000 import-id)]
        (swap! values-written inc)
        (when (= (mod @values-written 100000) 0)
          (log/info "Wrote" @values-written "values"))
        (let [path (.getValue path)
              simple_path (.getValue simple_path)
              escaped-value (StringEscapeUtils/escapeXml value)]
          (if-let [attribute (attr path)]
            (do
              (if @inside-open-tag
                (if (same-tag? @last-seen-path path)
                  (.write f (str " " attribute "=\"" escaped-value "\""))
                  (let [{:keys [to-close
                                to-open]} (to-close-and-to-open @last-seen-path path)
                        last-tag (last to-open)
                        tags-to-open (butlast to-open)]
                    (.write f ">")
                    (.write f (closing-tags to-close))
                    (.write f (opening-tags tags-to-open))
                    (.write f (str "<" last-tag " " attribute "=\"" escaped-value "\""))))
                (let [{:keys [to-close
                              to-open]} (to-close-and-to-open @last-seen-path path)
                      last-tag (last to-open)
                      tags-to-open (butlast to-open)]
                  (.write f (closing-tags to-close))
                  (.write f (opening-tags tags-to-open))
                  (.write f (str "<" last-tag " " attribute "=\"" escaped-value "\""))))
              (reset! inside-open-tag true))
            (do
              (when @inside-open-tag
                (.write f ">"))
              (let [{:keys [to-close
                            to-open]} (to-close-and-to-open @last-seen-path path)]
                (.write f (closing-tags to-close))
                (.write f (opening-tags to-open))
                (.write f escaped-value))
              (reset! inside-open-tag false)))
          (reset! last-seen-path path)))
      (let [{:keys [to-close]} (to-close-and-to-open @last-seen-path "")]
        (when @inside-open-tag
          (.write f ">"))
        (.write f (closing-tags to-close))))))

(defn generate-xml-file [{:keys [spec-version import-id xml-output-file] :as ctx}]
  (write-xml xml-output-file @spec-version import-id)
  ctx)

(def pipeline
  [create-xml-file
   generate-xml-file])
