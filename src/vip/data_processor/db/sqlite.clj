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
     :entities {}}))
