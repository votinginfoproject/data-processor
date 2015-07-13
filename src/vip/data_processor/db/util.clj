(ns vip.data-processor.db.util
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [korma.core :as korma]))

(defn kw->table-name [kw]
  (-> kw
      name
      (str/replace "-" "_")))

(defn simple-entity [db ent]
  (-> (korma/create-entity (kw->table-name ent))
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

(defn make-entities [db entity-names]
  (zipmap entity-names (map (partial simple-entity db) entity-names)))

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

(defn bulk-import [statement-parameter-limit table rows]
  (log/info "Bulk importing" (:name table))
  (doseq [chunk (chunk-rows rows statement-parameter-limit)]
    (when-not (empty? chunk)
      (korma/insert table (korma/values chunk)))))

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
