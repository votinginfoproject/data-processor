(ns vip.data-processor.output.street-segments
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [me.raynes.fs :as fs]
   [vip.data-processor.util :as util])
  (:import
   [javax.xml.stream XMLInputFactory XMLOutputFactory XMLEventFactory]))

(defn read-one-line
  "Reads one line at a time from a reader and parses it as a CSV
  string. Does NOT close the reader at the end."
  [reader]
  (->> reader .readLine csv/read-csv first))

(defn header-row
  "Given a file handle, returns a collection of cleaned (of non word
  characters), lower-cased string keys for the first line, assumed to
  be the header row."
  [file-handle]
  (->> file-handle
       read-one-line
       (map #(str/replace % #"\W" ""))
       (map (comp keyword str/lower-case))))

(defn add-element
  [event-factory writer el-name el-content]
  (when-not (str/blank? el-content)
    (.add writer (.createStartElement event-factory "" nil el-name))
    (.add writer (.createCharacters event-factory el-content))
    (.add writer (.createEndElement event-factory "" nil el-name))))

(def ss-fields
  {:address_direction "AddressDirection"
   :city "City"
   :includes_all_addresses "IncludesAllAddresses"
   :includes_all_streets "IncludesAllStreets"
   :odd_even_both "OddEvenBoth"
   :precinct_id "PrecinctId"
   :start_house_number "StartHouseNumber"
   :end_house_number "EndHouseNumber"
   :state "State"
   :street_direction "StreetDirection"
   :street_name "StreetName"
   :street_suffix "StreetSuffix"
   :unit_number "UnitNumber"
   :zip "Zip"})

(defn street-segments
  [event-factory writer street-segment-file]
  (with-open [in-file (util/bom-safe-reader street-segment-file :encoding "UTF-8")]
    (let [header-row (header-row in-file), line-count (atom 1)]
      (log/info "HEADER ROW: " header-row)
      (doseq [line (csv/read-csv in-file)]
        (let [line' (zipmap header-row line)]
          (swap! line-count inc)
          (.add writer (.createStartElement event-factory "" nil "StreetSegment"))
          (.add writer (.createAttribute event-factory "id" (:id line')))
          (doseq [[ss-key ss-name] ss-fields]
            (add-element event-factory writer ss-name (ss-key line')))
          (.add writer (.createEndElement event-factory "" nil "StreetSegment")))))))

(defn process-xml
  [{:keys [xml-output-file] :as ctx}]
  (if-let [ss-file (util/find-input-file ctx "street_segment.txt")]
    (let [tmpfile (fs/temp-file "output-xml-tmp-")
          reader (->> (.toFile xml-output-file)
                      (.createXMLEventReader (XMLInputFactory/newInstance)))
          writer (->> (io/writer tmpfile)
                      (.createXMLEventWriter (XMLOutputFactory/newInstance)))
          event-fact (XMLEventFactory/newInstance)]
      (while (.hasNext reader)
        (let [event (.nextEvent reader)]
          (when (and (.isEndElement event)
                     (= (str (.getName (.asEndElement event))) "VipObject"))
            (street-segments event-fact writer ss-file))
          (.add writer event)))
      (.close reader)
      (.close writer)
      (-> (assoc ctx :xml-output-file (.toPath tmpfile))
          ;; Not sure if we care about this, we may want to just throw it away
          (assoc :xml-output-file-old xml-output-file)))
    ctx))

(defn insert-street-segment-nodes
  [{:keys [post-process-street-segments?] :as ctx}]
  (if post-process-street-segments?
    (do (log/info "Inserting StreetSegments into XML output file.")
        (process-xml ctx))
    ctx))
