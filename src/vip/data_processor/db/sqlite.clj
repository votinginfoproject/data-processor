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
              :election-administrations (entity :election_administrations (korma/database db))}}))

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
