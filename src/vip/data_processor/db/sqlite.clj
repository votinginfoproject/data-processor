(ns vip.data-processor.db.sqlite
  (:require [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [vip.data-processor.db.util :as util])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-temp-db-file [prefix]
  (Files/createTempFile (str prefix "-") ".db" (into-array FileAttribute [])))

(defn temp-db [upload-filename]
  (let [temp-file (create-temp-db-file upload-filename)
        url (str "jdbc:sqlite:" temp-file)
        db (db/sqlite3 {:db temp-file})]
    (j/migrate-db {:db {:type :sql
                        :url url}
                   :migrator "resources/processing-migrations"})
    {:db db
     :tables (util/make-entities db util/import-entity-names)}))

(defn column-names
  "Find the names of all columns for a table. Uses a JDBC connection
  directly."
  [table]
  (let [url (str "jdbc:sqlite:" (get-in table [:db :db]))]
    (with-open [conn (java.sql.DriverManager/getConnection url)]
      (let [statement (.createStatement conn)
            result-set (.executeQuery statement (str "PRAGMA table_info(" (:name table) ")"))]
        (loop [columns []]
          (if (.next result-set)
            (recur (conj columns (.getString result-set "name")))
            columns))))))

(def statement-parameter-limit 999)
(def bulk-import (partial util/bulk-import statement-parameter-limit))
