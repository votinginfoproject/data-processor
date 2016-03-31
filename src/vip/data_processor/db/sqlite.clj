(ns vip.data-processor.db.sqlite
  (:require [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [vip.data-processor.db.util :as util]
            [clojure.java.jdbc :as jdbc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-temp-db-file [import-id]
  (Files/createTempFile (str "import-" import-id "-") ".db" (into-array FileAttribute [])))

(defn temp-db [import-id version]
  (let [temp-file (create-temp-db-file import-id)
        url (str "jdbc:sqlite:" temp-file)
        db (db/sqlite3 {:db temp-file})]
    (j/migrate-db {:db {:type :sql
                        :url url}
                   :migrator (str "resources/processing-migrations/v" version)})
    {:db db
     :tables (util/make-entities version db util/import-entity-names)}))

(defn column-names
  "Find the names of all columns for a table."
  [table]
  (let [db (:db table)]
    (jdbc/with-db-metadata [md db]
      (let [results (jdbc/metadata-result
                     (.getColumns md nil nil (:name table) nil))]
        (mapv :column_name results)))))

(def statement-parameter-limit 500)
(def bulk-import (partial util/bulk-import statement-parameter-limit))

(defn attach-sqlite-db [{:keys [import-id spec-version] :as ctx}]
  (let [db (temp-db import-id spec-version)
        db-file (get-in db [:db :db])]
    (-> ctx
        (merge db)
        (update :to-be-cleaned conj db-file))))
