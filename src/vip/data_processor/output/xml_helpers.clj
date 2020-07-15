(ns vip.data-processor.output.xml-helpers
  (:require [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.util :as util])
  (:import [java.nio.file Files StandardCopyOption]
           [java.nio.file.attribute FileAttribute]))

(defn format-fips [fips]
  (cond
    (empty? fips) "XX"
    (< (count fips) 3) (format "%02d" (Integer/parseInt fips))
    (< (count fips) 5) (format "%05d" (Integer/parseInt fips))
    :else fips))

(defn format-state
  [state]
  (if (empty? state)
    "YY"
    (clojure.string/replace (clojure.string/trim state) #"\s" "-")))

(defn filename* [fips state election-date]
  (let [fips (format-fips fips)
        state (format-state state)
        date (util/format-date election-date)]
    (str/join "-" ["vipfeed" fips state date])))

(defn generate-file-basename
  [{:keys [spec-family tables import-id] :as ctx}]
  (condp = spec-family
    "3.0"
    (let [fips (-> tables
                   :sources
                   korma/select
                   first
                   :vip_id)
          state (-> tables
                    :states
                    korma/select
                    first
                    :name)
          election-date (-> tables
                            :elections
                            korma/select
                            first
                            :date)]
      (assoc ctx :output-file-basename
             (filename* fips state election-date)))

    "5.2"
    (let [fips (postgres/find-value-for-simple-path
                import-id "VipObject.Source.VipId")
          state (postgres/find-value-for-simple-path
                 import-id "VipObject.State.Name")
          election-date (postgres/find-value-for-simple-path
                         import-id "VipObject.Election.Date")]
      (assoc ctx :output-file-basename
             (filename* fips state election-date)))))

(defn create-xml-file
  "Creates a temp file for the output xml, then moves it to a name without
   the randomness that temp files get automatically added to their names."
  [{:keys [import-id output-file-basename] :as ctx}]
  (let [tmp-xml-file (Files/createTempFile
                      (str import-id) ".xml" (into-array FileAttribute []))
        xml-file (.resolveSibling tmp-xml-file
                                  (str output-file-basename ".xml"))
        xml-file (Files/move tmp-xml-file xml-file
                             (into-array
                              [StandardCopyOption/REPLACE_EXISTING]))]
    (-> ctx
        (assoc :xml-output-file xml-file)
        (update :to-be-cleaned conj xml-file))))

(defmacro xml-node [name]
  (let [tag (keyword name)]
    `{:tag ~tag :content [(str ~name)]}))

(defmacro boolean-xml-node [name]
  (let [tag (keyword name)]
    `{:tag ~tag :content [(condp = ~name
                            1 "yes"
                            0 "no"
                            nil)]}))

(defn empty-content? [c]
  (or (nil? c)
      (empty? (first (:content c)))))

(defn remove-empties [content]
  (remove empty-content? content))

(defn simple-xml
  ([tag content]
   {:tag tag :content (remove-empties content)})
  ([tag id content]
   {:tag tag :attrs {:id id} :content (remove-empties content)}))

(defn id-keyword [s]
  (-> s
      name
      (str/replace "-" "_")
      (str "_id")
      keyword))

(defn joined-nodes
  "Create XML nodes for a source element from a join table.

  For example:
       (joined-nodes ctx :precinct 90047 :polling-location)

  That will create nodes like `{:tag :polling_location_id :content [\"10029\"]}`
  for every entry in the precinct_polling_locations table with
  precinct_id = 90047."
  [ctx source source-id join]
  (let [source-name (name source)
        join-name (name join)
        join-table-key (keyword (str source-name "-" join-name "s"))
        join-table (get-in ctx [:tables join-table-key])
        source-id-column (id-keyword source-name)
        tag (id-keyword join-name)
        join-rows (korma/select join-table (korma/where {source-id-column source-id}))]
    (map (fn [join-row] {:tag tag :content [(str (get join-row tag))]}) join-rows)))
