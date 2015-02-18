(ns vip.data-processor.db.postgres
  (:require [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [korma.core :as korma]
            [turbovote.resource-config :refer [config]]))

(defn url []
  (let [{:keys [host port user password]} (config :postgres)]
    (str "jdbc:postgresql://" host ":" port "/?user=" user "&password=" password)))

(defn migrate []
  (j/migrate-db {:db {:type :sql
                      :url (url)}
                 :migrator "resources/migrations"}))

(declare results-db results)

(defn initialize []
  (migrate)
  (db/defdb results-db (db/postgres (-> :postgres
                                        config
                                        (assoc :db (config :postgres :user)))))
  (korma/defentity results
    (korma/database results-db)))
