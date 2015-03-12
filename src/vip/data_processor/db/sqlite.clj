(ns vip.data-processor.db.sqlite
  (:require [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [korma.core :as korma])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-temp-db-file [prefix]
  (Files/createTempFile (str prefix "-") ".db" (into-array FileAttribute [])))

(defmacro entity [ent & body]
  `(-> (korma/create-entity ~(name ent))
       ~@body))

(defn temp-db [upload-filename]
  (let [temp-file (create-temp-db-file upload-filename)
        url (str "jdbc:sqlite:" temp-file)
        db (db/sqlite3 {:db temp-file})]
    (j/migrate-db {:db {:type :sql
                        :url url}
                   :migrator "resources/processing-migrations"})
    {:db db
     :tables {:elections (entity :elections (korma/database db))
              :sources (entity :sources (korma/database db))
              :states (entity :states (korma/database db))
              :election-administrations (entity :election_administrations (korma/database db))
              :election-officials (entity :election_officials (korma/database db))
              :localities (entity :localities (korma/database db))
              :precincts (entity :precincts (korma/database db))
              :precinct-splits (entity :precinct_splits (korma/database db))
              :precinct-split-electoral-districts (entity :precinct_split_electoral_districts (korma/database db))
              :early-vote-sites (entity :early_vote_sites (korma/database db))
              :precinct-polling-locations (entity :precinct_polling_locations (korma/database db))
              :polling-locations (entity :polling_locations (korma/database db))
              :street-segments (entity :street_segments (korma/database db))
              :electoral-districts (entity :electoral_districts (korma/database db))
              :contests (entity :contests (korma/database db))
              :ballots (entity :ballots (korma/database db))
              :ballot-responses (entity :ballot_responses (korma/database db))
              :referendums (entity :referendums (korma/database db))
              :referendum-ballot-responses (entity :referendum_ballot_responses (korma/database db))
              :candidates (entity :candidates (korma/database db))
              :ballot-candidates (entity :ballot_candidates (korma/database db))
              :state-early-vote-sites (entity :state_early_vote_sites (korma/database db))
              :precinct-split-polling-locations (entity :precinct_split_polling_locations (korma/database db))
              :precinct-electoral-districts (entity :precinct_electoral_districts (korma/database db))
              :precinct-early-vote-sites (entity :precinct_early_vote_sites (korma/database db))
              :locality-early-vote-sites (entity :locality_early_vote_sites (korma/database db))
              :custom-ballot-ballot-responses (entity :custom_ballot_ballot_responses (korma/database db))
              :custom-ballots (entity :custom_ballots (korma/database db ))
              :contest-results (entity :contest_results (korma/database db))}}))

(defn column-names
  "Find the names of all columns for a table. Uses a JDBC connection
  directly."
  [db table]
  (let [url (str "jdbc:sqlite:" (:db db))]
    (with-open [conn (java.sql.DriverManager/getConnection url)]
      (let [statement (.createStatement conn)
            result-set (.executeQuery statement (str "PRAGMA table_info(" table ")"))]
        (loop [columns []]
          (if (.next result-set)
            (recur (conj columns (.getString result-set "name")))
            columns))))))

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

(def statement-parameter-limit 999)

(defn bulk-import [rows table]
  (doseq [chunk (chunk-rows rows statement-parameter-limit)]
    (when-not (empty? chunk)
      (korma/insert table (korma/values chunk)))))
