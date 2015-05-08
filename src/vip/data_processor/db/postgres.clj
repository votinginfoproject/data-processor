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

(declare results-db results
         validations-db validations)

(defn initialize []
  (migrate)
  (let [opts (-> :postgres
                 config
                 (assoc :db (config :postgres :user)))]
    (db/defdb results-db (db/postgres opts))
    (db/defdb validations-db (db/postgres opts)))
  (korma/defentity results
    (korma/database results-db))
  (korma/defentity validations
    (korma/database validations-db)))

(defn start-run [ctx]
  (let [results (korma/insert results
                              (korma/values {:start_time (korma/sqlfn now)
                                             :complete false}))]
    (assoc ctx :import-id (:id results))))

(defn complete-run [ctx]
  (let [id (:import-id ctx)
        filename (:generated-xml-filename ctx)]
    (korma/update results
                  (korma/set-fields {:filename filename
                                     :complete true
                                     :end_time (korma/sqlfn now)})
                  (korma/where {:id id}))))

(defn get-run [ctx]
  (korma/select results
                (korma/where {:id (:import-id ctx)})))

(defn insert-validation [id severity scopes scope-key]
  (let [scope (get scopes scope-key)
        description (-> scope keys first)
        message (-> scope vals first)]
    (korma/insert validations
                  (korma/values {:result_id id
                                 :severity (name severity)
                                 :scope (if (keyword? scope-key)
                                          (name scope-key)
                                          scope-key)
                                 :description (if (keyword? description)
                                                (name description)
                                                description)
                                 :message (apply str message)}))))

(defn insert-validations [{:keys [warnings errors critical fatal] :as ctx}]
  (let [result-id (:import-id ctx)
        insert-severity-fn (fn [type scopes]
                             (pmap (partial insert-validation result-id type scopes)
                                   (keys scopes)))]
    (when warnings (insert-severity-fn 'warnings warnings))
    (when errors (insert-severity-fn 'errors errors))
    (when critical (insert-severity-fn 'critical critical))
    (when fatal (insert-severity-fn 'fatal fatal))
    ctx))
