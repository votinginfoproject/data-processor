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
            [vip.data-processor.validation.data-spec :as data-spec])
  (:import [org.postgresql.util PGobject]))

(defn url []
  (let [{:keys [host port user password database]} (config [:postgres])]
    (str "jdbc:postgresql://" host ":" port "/" database "?user=" user "&password=" password)))

(defn migrate []
  (j/migrate-db {:db {:type :sql
                      :url (url)}
                 :migrator "resources/migrations"}))

(declare results-db results
         validations
         v3-0-import-entities
         statistics)

(defn initialize []
  (log/info "Initializing Postgres")
  (migrate)
  (let [opts (-> [:postgres]
                 config
                 (assoc :db (config [:postgres :database])))]
    (db/defdb results-db (db/postgres opts)))
  (korma/defentity results
    (korma/database results-db))
  (korma/defentity validations
    (korma/database results-db))
  (korma/defentity statistics
    (korma/database results-db))
  (korma/defentity election_approvals
    (korma/database results-db))
  (korma/defentity xml-tree-values
    (korma/table "xml_tree_values")
    (korma/database results-db))
  (korma/defentity xml-tree-validations
    (korma/table "xml_tree_validations")
    (korma/database results-db))
  (korma/defentity v5-0-street-segments
    (korma/table "v5_0_street_segments"))
  (def v3-0-import-entities
    (db.util/make-entities "3.0" results-db db.util/import-entity-names)))

(defn path->ltree [path]
  (doto (PGobject.)
    (.setType "ltree")
    (.setValue path)))

(defn ltree-match
  "Helper function for generating WHERE clases using ~. Accepts a keyword as a
  table alias, or a korma entity"
  [table column path]
  (let [tablename (if (keyword? table)
                    (name table)
                    (korma.sql.engine/table-alias table))]
    (korma/raw (str tablename
                    "." (name column)
                    " ~ '" path "'"))))

(defn start-run [ctx]
  (let [results (korma/insert results
                              (korma/values {:start_time (korma/sqlfn now)
                                             :complete false}))]
    (assoc ctx :import-id (:id results))))

(defn build-public-id [date election-type state import-id]
  (let [nil-or-empty? (some-fn nil? empty?)
        formatted-date (util/format-date date)
        good-parts (->> [formatted-date election-type state]
                        (remove nil-or-empty?)
                        (map #(str/trim %)))]
    (if (empty? good-parts)
      (str "invalid-" import-id)
      (str/join "-" (concat good-parts [import-id])))))

(defn build-election-id [date election-type state]
  (let [components [date election-type state]]
    (when (every? seq components)
      (->> components
           (map str/trim)
           (str/join "-")))))

(defn get-v3-public-id-data [{:keys [import-id] :as ctx}]
  (let [state (-> ctx
                  (get-in [:tables :states])
                  (korma/select (korma/fields :name))
                  first
                  :name)
        {:keys [date election_type]} (-> ctx
                                         (get-in [:tables :elections])
                                         (korma/select (korma/fields :date :election_type))
                                         first)]
    {:date date
     :election-type election_type
     :state state
     :import-id import-id}))

(defn find-single-xml-tree-value [results-id path]
  (-> (korma/select xml-tree-values
        (korma/fields :value)
        (korma/where {:results_id results-id})
        (korma/where (ltree-match xml-tree-values
                                  :path
                                  path)))
      first
      :value))

(defn get-xml-tree-public-id-data [{:keys [import-id] :as ctx}]
  (let [state (find-single-xml-tree-value
               import-id
               "VipObject.0.State.*{1}.Name.*{1}")
        date (find-single-xml-tree-value
              import-id
              "VipObject.0.Election.*{1}.Date.*{1}")
        election-type (find-single-xml-tree-value
                       import-id
                       "VipObject.0.Election.*{1}.ElectionType.*{1}")]
    {:date date
     :election-type election-type
     :state state
     :import-id import-id}))

(defn get-public-id-data [{:keys [spec-version] :as ctx}]
  (condp = spec-version
    "3.0" (get-v3-public-id-data ctx)
    "5.0" (get-xml-tree-public-id-data ctx)
    {}))

(defn generate-public-id [ctx]
  (let [{:keys [date election-type state import-id]} (get-public-id-data ctx)]
    (build-public-id date election-type state import-id)))

(defn generate-election-id [ctx]
  (let [{:keys [date election-type state]} (get-public-id-data ctx)]
    (build-election-id date election-type state)))

(defn store-public-id [ctx]
  (let [id (:import-id ctx)
        public-id (generate-public-id ctx)]
    (korma/update results
                  (korma/set-fields {:public_id public-id})
                  (korma/where {:id id}))
    (assoc ctx :public-id public-id)))

(defn save-election-id! [election-id]
  (binding [db/*current-conn* (db/get-connection (:db election_approvals))]
    (db/transaction
     (when-not (seq (korma/select election_approvals
                                  (korma/where {:election_id election-id})))
       (korma/insert election_approvals
                     (korma/values {:election_id election-id}))))))

(defn store-election-id [ctx]
  (if-let [election-id (generate-election-id ctx)]
    (let [id (:import-id ctx)]
      (save-election-id! election-id)
      (korma/update results
                    (korma/set-fields {:election_id election-id})
                    (korma/where {:id id}))
      (assoc ctx :election-id election-id))
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
                (korma/set-fields {:exception exception
                                   :end_time (korma/sqlfn now)})
                (korma/where {:id id})))

(defn get-run [ctx]
  (korma/select results
                (korma/where {:id (:import-id ctx)})))

(def global-identifier -1)
(def invalid-identifier -2)

(def coercable-identifier?
  (some-fn string? number? nil? #{:global}))

(defn coerce-identifier
  "Coerce an error identifier to something that can be inserted as a
  Postgres BIGINT or NULL"
  [identifier]
  (cond
    (not (coercable-identifier? identifier)) invalid-identifier
    (= :global identifier) global-identifier
    (string? identifier) (try
                           (BigDecimal. identifier)
                           (catch java.lang.NumberFormatException _
                             invalid-identifier))
    :else identifier))

(defn validation-value [results-id severity scope identifier error-type error-data]
  {:results_id results-id
   :severity (name severity)
   :scope (name scope)
   :identifier (coerce-identifier identifier)
   :error_type (name error-type)
   :error_data (pr-str error-data)})

(defn xml-tree-validation-value
  [results-id severity scope path error-type error-data]
  {:results_id results-id
   :severity (name severity)
   :scope (name scope)
   :error_type (name error-type)
   :error_data (pr-str error-data)
   :path (when-not (= :global path) (path->ltree path))})

(defn validation-values-from [values-fn]
  (fn [{:keys [import-id] :as ctx}]
    (mapcat
     (fn [severity]
       (let [errors (get ctx severity)]
         (when-not (empty? errors)
           (let [errors (util/flatten-keys errors)]
             (mapcat (fn [[[scope identifier error-type] error-data]]
                       (map (fn [error-data]
                              (values-fn import-id severity scope identifier error-type error-data))
                            error-data))
                     errors)))))
     [:warnings :errors :critical :fatal])))

(def validation-values
  "Create insertable validations from the processing context map. Suitable for the 3.0 schema.

    (validation-values {:import-id 493
                        :errors {:candidates {3 {:missing-values [name\" \"email\"]}}}
                        :critical {:candidates {:global {:missing-columns [\"party\"]}}}})
    ;; =>
       ({:results_id 493 :severity \"errors\" :scope \"candidates\" :identifier 3 :error_type \"missing-values\" :error_data \"\\\"name\\\"\"}
        {:results_id 493 :severity \"errors\" :scope \"candidates\" :identifier 3 :error_type \"missing-values\" :error_data \"\\\"email\\\"\"}
        {:results_id 493 :severity \"critical\" :scope \"candidates\" :identifier -1 :error_type \"missing-columns\" :error_data \"\\\"party\\\"\"})"
  (validation-values-from validation-value))

(def xml-tree-validation-values
  "Create insertable validations from the processing context map. Suitable for the 5.0 schema."
  (validation-values-from xml-tree-validation-value))

(def statement-parameter-limit 10000)
(def bulk-import (partial db.util/bulk-import statement-parameter-limit))

(defn insert-validations [ctx]
  (log/info "Inserting validations")
  (let [validation-values (validation-values ctx)]
    (bulk-import ctx validations validation-values)))

(defn import-from-sqlite [{:keys [import-id db data-specs] :as ctx}]
  (reduce (fn [ctx ent]
            (let [table (get-in ctx [:tables ent])
                  columns (->> data-specs
                               (filter #(= ent (:table %)))
                               first
                               :columns)]
              (bulk-import ctx
                           (ent v3-0-import-entities) ; TODO: choose import-entities based on import version
                           (->> table
                                (db.util/select-*-lazily 5000)
                                (map #(assoc % :results_id import-id))
                                (data-spec/coerce-rows columns)))))
          ctx
          db.util/import-entity-names))

(defn store-stats [{:keys [import-id] :as ctx}]
  (korma/insert statistics
                (korma/values (assoc (stats/stats-map ctx) :results_id import-id)))
  ctx)
