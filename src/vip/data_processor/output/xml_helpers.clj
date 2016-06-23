(ns vip.data-processor.output.xml-helpers
  (:require [korma.core :as korma])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-xml-file
  [{:keys [filename] :as ctx}]
  (let [xml-file (Files/createTempFile filename ".xml" (into-array FileAttribute []))]
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
      (clojure.string/replace "-" "_")
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
