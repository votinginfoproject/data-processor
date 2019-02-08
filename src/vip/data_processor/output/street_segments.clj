(ns vip.data-processor.output.street-segments
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [korma.core :as korma]
   [me.raynes.fs :as fs]
   [vip.data-processor.db.postgres :as psql]
   [vip.data-processor.errors :as errors]
   [vip.data-processor.util :as util])
  (:import
   [javax.xml.stream XMLInputFactory XMLOutputFactory XMLEventFactory]))

(defn add-element
  [event-factory writer el-name el-content]
  (when-not (str/blank? el-content)
    (.add writer (.createStartElement event-factory "" nil el-name))
    (.add writer (.createCharacters event-factory el-content))
    (.add writer (.createEndElement event-factory "" nil el-name))))

(def ss-ordered-fields
  [[:address_direction "AddressDirection"]
   [:city "City"]
   [:includes_all_addresses "IncludesAllAddresses"]
   [:includes_all_streets "IncludesAllStreets"]
   [:odd_even_both "OddEvenBoth"]
   [:precinct_id "PrecinctId"]
   [:start_house_number "StartHouseNumber"]
   [:end_house_number "EndHouseNumber"]
   [:state "State"]
   [:street_direction "StreetDirection"]
   [:street_name "StreetName"]
   [:street_suffix "StreetSuffix"]
   [:unit_number "UnitNumber"]
   [:zip "Zip"]])

(def ss-fields
  (into {} ss-ordered-fields))

(defn validate-required-field!
  [ctx {:keys [id] :as street-segment-entry} field-key]
  (if (str/blank? (field-key street-segment-entry))
    (do (errors/record-error!
         ctx {:severity :errors
              :scope :street-segments
              :identifier :post-process-street-segments
              :error-type :missing-data
              :error-data [{:message (str "Missing " (field-key ss-fields)
                                          " for entry with id " id)
                            :missing-field (field-key ss-fields)
                            :data street-segment-entry}]})
        false)
    true))

(defn validate-precinct-id!
  [ctx {:keys [id precinct_id] :as street-segment-entry} precinct-ids]
  (when-not (precinct-ids precinct_id)
    (errors/record-error!
     ctx {:severity :errors
          :scope :street-segments
          :identifier :post-process-street-segments
          :error-type :missing-data
          :error-data [{:message (str "Precinct ID doesn't exist: " precinct_id
                                      " for entry with id " id)
                        :bad-precinct-id precinct_id
                        :data street-segment-entry}]})
    false))

(def required-fields
  [:id :precinct_id :city :state])

(defn validate-street-segment!
  "Runs some basic validations on a given street segment, including
  checking that required fields are present and that the precinct-id
  exists. Returns boolean `true` if the street segment is valid,
  `false` otherwise. Records errors for any invalidations."
  [ctx street-segment-entry precinct-ids]
  (->> (mapv
        ;; Check if required fields are missing
        #(validate-required-field! ctx street-segment-entry %)
        required-fields)
       (into
        ;; Check if we have a valid precinct-id
        [(validate-precinct-id! ctx street-segment-entry precinct-ids)])
       (every? identity)))

(defn header-row
  "Given a file handle (java.io.Reader), returns a collection of
  cleaned (of non word characters), lower-cased string keys for the
  first line, assumed to be the header row."
  [file-handle]
  (let [read-one-line #(->> % .readLine csv/read-csv first)]
    (->> file-handle
         read-one-line
         (map #(str/replace % #"\W" ""))
         (map (comp keyword str/lower-case)))))

(defn street-segments
  [ctx event-factory writer street-segment-file precinct-ids]
  (with-open [in-file (util/bom-safe-reader street-segment-file :encoding "UTF-8")]
    (let [header-row (header-row in-file)]
      (doseq [line (csv/read-csv in-file)]
        (let [line' (zipmap header-row line)]
          (validate-street-segment! ctx line' precinct-ids)
          (.add writer (.createStartElement event-factory "" nil "StreetSegment"))
          (.add writer (.createAttribute event-factory "id" (:id line')))
          (doseq [[ss-key ss-name] ss-ordered-fields]
            (add-element event-factory writer ss-name (ss-key line')))
          (.add writer (.createEndElement event-factory "" nil "StreetSegment")))))))

(defn load-precinct-ids
  []
  (jdbc/with-db-connection [conn (psql/db-spec)]
    (->> (korma/select (psql/v5-1-tables :precincts)
           (korma/fields :id))
         (reduce #(conj %1 (:id %2)) #{}))))

(defn process-xml
  [{:keys [xml-output-file] :as ctx}]
  (if-let [ss-file (util/find-input-file ctx "street_segment.txt")]
    (let [tmpfile (fs/temp-file "" ".xml")
          reader (->> (.toFile xml-output-file)
                      (.createXMLEventReader (XMLInputFactory/newInstance)))
          writer (->> (io/writer tmpfile)
                      (.createXMLEventWriter (XMLOutputFactory/newInstance)))
          event-fact (XMLEventFactory/newInstance)
          precinct-ids (load-precinct-ids)]
      (while (.hasNext reader)
        (let [event (.nextEvent reader)]
          (when (and (.isEndElement event)
                     (= (str (.getName (.asEndElement event))) "VipObject"))
            (street-segments ctx event-fact writer ss-file precinct-ids))
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
