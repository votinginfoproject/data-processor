(ns vip.data-processor.db.util
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.string :as str]
            [korma.core :as korma]
            [korma.db :as db]))

(defn sqlize-str [s]
  (-> s
      (str/replace "-" "_")
      (str/replace "." "_")))

(defn kw->table-name [version kw]
  (as-> kw
    table-name
    (name table-name)
    (str "v" version "_" table-name)
    (sqlize-str table-name)))

(defn simple-entity [version db ent]
  (-> (korma/create-entity (kw->table-name version ent))
      (assoc :alias (sqlize-str (name ent)))
      (assoc :kw ent)
      (korma/database db)))

(def import-entity-names
  [:ballot-candidates
   :ballot-line-results
   :ballot-responses
   :ballots
   :candidates
   :contest-results
   :contests
   :custom-ballot-ballot-responses
   :custom-ballots
   :early-vote-sites
   :election-administrations
   :election-officials
   :elections
   :electoral-districts
   :localities
   :locality-early-vote-sites
   :polling-locations
   :precinct-early-vote-sites
   :precinct-electoral-districts
   :precinct-polling-locations
   :precinct-split-electoral-districts
   :precinct-split-polling-locations
   :precinct-splits
   :precincts
   :referendum-ballot-responses
   :referendums
   :sources
   :state-early-vote-sites
   :states
   :street-segments])

(defn make-entities [version db entity-names]
  (zipmap entity-names (map (partial simple-entity version db) entity-names)))

(defn chunk-rows
  "Given a seq of maps, split them into groups, such that no group
  have more than n total keys across its maps."
  [rows n]
  (lazy-seq
   (loop [this-chunk []
          this-chunk-size 0
          to-go rows]
     (if (empty? to-go)
       (list this-chunk)
       (let [next-row (first to-go)
             size-with-next-row (+ this-chunk-size (count next-row))]
         (cond
          (< n (count next-row)) (throw (ex-info "Map too large" {:map next-row}))
          (< n size-with-next-row) (cons this-chunk (chunk-rows to-go n))
          :else (recur (conj this-chunk next-row)
                       size-with-next-row
                       (rest to-go))))))))

(defn retry-chunk-without-dupe-ids
  "If inserting a chunk has failed, retry trying to remove duplicate ids"
  [ctx sql-table chunk-values]
  ;; We need to query for existing ids and retry in the insert in a
  ;; single transaction to be sure we have a consistent view of what
  ;; ids already exist. Otherwise, rows that are still "in flight"
  ;; from the previous commit may get commited between this read and
  ;; write.
  ;;
  ;; Korma's `db/transaction` macro uses a dynamic var to choose which
  ;; database do the transaction on. We bind it here to ensure that it
  ;; uses the SQLite database for the table in question.
  (binding [db/*current-conn* (db/get-connection (:db sql-table))]
    (db/transaction
     (let [table (-> sql-table :name keyword)
           entity (-> sql-table :kw keyword)
           existing-ids (->> (korma/select sql-table
                                           (korma/fields :id)
                                           (korma/where {:id [in (map #(BigInteger. (get % "id")) chunk-values)]}))
                             (map (comp str :id))
                             set)
           local-dupe-ids (->> chunk-values
                               (map #(get % "id"))
                               frequencies
                               (filter (fn [[k v]] (> v 1)))
                               (map first)
                               set)
           chunk-without-dupe-ids (remove (fn [{:strs [id]}]
                                            (or (existing-ids id)
                                                (local-dupe-ids id)))
                                          chunk-values)]
       (when (seq chunk-without-dupe-ids)
         (korma/insert sql-table (korma/values chunk-without-dupe-ids)))
       (reduce (fn [ctx dupe-id]
                 (assoc-in ctx [:fatal entity dupe-id :duplicate-ids]
                           ["Duplicate id"]))
               ctx (set/union existing-ids local-dupe-ids))))))

(defn hydrate-row [ks row]
  (reduce (fn [row k]
            (if (contains? row k)
              row
              (assoc row k nil)))
          row ks))

(defn hydrate-rows [rows]
  (let [ks (apply clojure.set/union (map (comp set keys) rows))]
    (map (partial hydrate-row ks) rows)))

(defn bulk-import [statement-parameter-limit ctx table rows]
  (log/info "Bulk importing" (:alias table))
  (reduce (fn [ctx rows]
            (if (empty? rows)
              ctx
              (let [hydrated-rows (hydrate-rows rows)]
                (try
                  (korma/insert table (korma/values hydrated-rows))
                  ctx
                  (catch java.sql.SQLException e
                    (let [message (.getMessage e)]
                      (if (re-find #"UNIQUE constraint failed: (\w+).id" message)
                        (retry-chunk-without-dupe-ids ctx table hydrated-rows)
                        (assoc-in ctx [:fatal (:name table) :global :unknown-sql-error]
                                  [message]))))))))
          ctx (chunk-rows rows statement-parameter-limit)))

(defn select-*-lazily [chunk-size sql-table]
  (let [total (-> sql-table
                  (korma/select (korma/aggregate (count "*") :total))
                  first
                  :total)]
    (letfn [(chunked-rows [page]
              (let [offset (* page chunk-size)]
                (when (< offset total)
                  (lazy-cat
                   (korma/select sql-table
                                 (korma/offset offset)
                                 (korma/limit chunk-size))
                   (chunked-rows (inc page))))))]
      (chunked-rows 0))))
