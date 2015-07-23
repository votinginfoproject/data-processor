(ns vip.data-processor.db.postgres
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [korma.core :as korma]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.statistics :as stats]
            [vip.data-processor.db.util :as db.util]
            [vip.data-processor.util :as util]
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
    (db.util/make-entities results-db db.util/import-entity-names)))

(defn start-run [ctx]
  (let [results (korma/insert results
                              (korma/values {:start_time (korma/sqlfn now)
                                             :complete false}))]
    (assoc ctx :import-id (:id results))))

(defn build-public-id [date election-type state import-id]
  (let [nil-or-empty? (some-fn nil? empty?)
        good-parts (remove nil-or-empty? [date election-type state])]
    (if (empty? good-parts)
      (str "invalid-" import-id)
      (str/join "-" (concat good-parts [import-id])))))

(defn generate-public-id [{:keys [import-id] :as ctx}]
  (let [state (-> ctx
                 (get-in [:tables :states])
                 (korma/select (korma/fields :name))
                 first
                 :name)
        {:keys [date election_type]} (-> ctx
                                         (get-in [:tables :elections])
                                         (korma/select (korma/fields :date :election_type))
                                         first)]
    (build-public-id date election_type state import-id)))

(defn store-public-id [ctx]
  (let [id (:import-id ctx)
        public-id (generate-public-id ctx)]
    (korma/update results
                  (korma/set-fields {:public_id public-id})
                  (korma/where {:id id}))
    ctx))

(defn complete-run [ctx]
  (let [id (:import-id ctx)
        filename (:generated-xml-filename ctx)]
    (korma/update results
                  (korma/set-fields {:filename filename
                                     :complete true
                                     :end_time (korma/sqlfn now)})
                  (korma/where {:id id}))))

(defn fail-run [id exception]
  (korma/update results
                (korma/set-fields {:exception exception})
                (korma/where {:id id})))

(defn get-run [ctx]
  (korma/select results
                (korma/where {:id (:import-id ctx)})))

(def global-identifier -1)

(def coercable-identifier?
  (some-fn string? number? nil? #{:global}))

(defn coerce-identifier
  "Coerce an error identifier to something that can be inserted as a
  Postgres BIGINT or NULL"
  [identifier]
  {:pre [(coercable-identifier? identifier)]}
  (cond
    (= :global identifier) global-identifier
    (string? identifier) (BigDecimal. identifier)
    :else identifier))

(defn validation-value [results-id severity scope identifier error-type error-data]
  {:results_id results-id
   :severity (name severity)
   :scope (name scope)
   :identifier (coerce-identifier identifier)
   :error_type (name error-type)
   :error_data (pr-str error-data)})

(defn validation-values
  "Create insertable validations from the processing context map.

    (validation-values {:import-id 493
                                  :errors {:candidates {3 {:missing-values [name\" \"email\"]}}}
                                  :critical {:candidates {:global {:missing-columns [\"party\"]}}}})
    ;; =>
       ({:results_id 493 :severity \"errors\" :scope \"candidates\" :identifier 3 :error_type \"missing-values\" :error_data \"\\\"name\\\"\"}
        {:results_id 493 :severity \"errors\" :scope \"candidates\" :identifier 3 :error_type \"missing-values\" :error_data \"\\\"email\\\"\"}
        {:results_id 493 :severity \"critical\" :scope \"candidates\" :identifier -1 :error_type \"missing-columns\" :error_data \"\\\"party\\\"\"})"
  [{:keys [import-id] :as ctx}]
  (mapcat
   (fn [severity]
     (let [errors (get ctx severity)]
       (when-not (empty? errors)
         (let [errors (util/flatten-keys errors)]
           (mapcat (fn [[[scope identifier error-type] error-data]]
                     (map (fn [error-data]
                               (validation-value import-id severity scope identifier error-type error-data))
                             error-data))
                   errors)))))
   [:warnings :errors :critical :fatal]))

(def statement-parameter-limit 10000)
(def bulk-import (partial db.util/bulk-import statement-parameter-limit))

(defn insert-validations [ctx]
  (log/info "Inserting validations")
  (let [validation-values (validation-values ctx)]
    (bulk-import validations validation-values))
  ctx)

(defn import-from-sqlite [{:keys [import-id db data-specs] :as ctx}]
  (doseq [ent db.util/import-entity-names]
    (let [table (get-in ctx [:tables ent])
          columns (->> data-specs
                       (filter #(= ent (:table %)))
                       first
                       :columns)]
      (bulk-import (ent import-entities)
                   (->> table
                        (db.util/select-*-lazily 5000)
                        (map #(assoc % :results_id import-id))
                        (data-spec/coerce-rows columns)))))
  ctx)

(defn store-stats [{:keys [import-id] :as ctx}]
  (korma/insert statistics
                (korma/values (assoc (stats/stats-map ctx) :results_id import-id)))
  ctx)
