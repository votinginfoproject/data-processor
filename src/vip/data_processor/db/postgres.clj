(ns vip.data-processor.db.postgres
  (:require [clojure.tools.logging :as log]
            [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [korma.core :as korma]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.statistics :as stats]
            [vip.data-processor.db.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]))

(defn url []
  (let [{:keys [host port user password]} (config :postgres)]
    (str "jdbc:postgresql://" host ":" port "/?user=" user "&password=" password)))

(defn migrate []
  (j/migrate-db {:db {:type :sql
                      :url (url)}
                 :migrator "resources/migrations"}))

(declare results-db results
         validations-db validations
         import-entities
         statistics)

(defn initialize []
  (log/info "Initializing Postgres")
  (migrate)
  (let [opts (-> :postgres
                 config
                 (assoc :db (config :postgres :user)))]
    (db/defdb results-db (db/postgres opts))
    (db/defdb validations-db (db/postgres opts)))
  (korma/defentity results
    (korma/database results-db))
  (korma/defentity validations
    (korma/database validations-db))
  (korma/defentity statistics
    (korma/database results-db))
  (def import-entities
    (util/make-entities results-db util/postgres-import-entity-names)))

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
                                 :message (pr-str message)}))))

(defn insert-validations [{:keys [warnings errors critical fatal] :as ctx}]
  (log/info "Inserting validations")
  (let [result-id (:import-id ctx)
        insert-severity-fn (fn [type scopes]
                             (doseq [scope (keys scopes)]
                               (insert-validation result-id type scopes scope)))]
    (when warnings (insert-severity-fn 'warnings warnings))
    (when errors (insert-severity-fn 'errors errors))
    (when critical (insert-severity-fn 'critical critical))
    (when fatal (insert-severity-fn 'fatal fatal))
    ctx))

(def statement-parameter-limit 3000)
(def bulk-import (partial util/bulk-import statement-parameter-limit))

(defn import-from-sqlite [{:keys [import-id db data-specs] :as ctx}]
  (doseq [ent util/postgres-import-entity-names]
    (let [table (get-in ctx [:tables ent])
          vals (korma/select table)
          vals (map #(assoc % :results_id import-id) vals)
          columns (->> data-specs
                       (filter #(= ent (:table %)))
                       first
                       :columns)
          vals (data-spec/coerce-rows columns vals)]
      (when (seq vals)
        (bulk-import (ent import-entities) vals))))
  ctx)

(defn store-stats [{:keys [import-id] :as ctx}]
  (korma/insert statistics
                (korma/values (assoc (stats/stats-map ctx) :results_id import-id)))
  ctx)
