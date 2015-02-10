(ns vip.data-processor.db.postgres
  (:require [joplin.core :as j]
            [joplin.jdbc.database]
            [turbovote.resource-config :refer [config]]))

(defn url []
  (let [{:keys [host port user password]} (config :postgres)]
    (str "jdbc:postgresql://" host ":" port "/?user=" user "&password=" password)))

;;; then, actual migrations
(defn migrate []
  (j/migrate-db {:db {:type :sql
                      :url (url)}
                 :migrator "migrations"}))
