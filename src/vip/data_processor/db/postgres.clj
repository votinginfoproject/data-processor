(ns vip.data-processor.db.postgres
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [joplin.core :as j]
            [joplin.jdbc.database]
            [korma.db :as db]
            [korma.core :as korma]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.util :as db.util]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]
            [clojure.set :as set])
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
  (korma/defentity v5-statistics
    (korma/table "v5_statistics")
    (korma/database results-db))
  (korma/defentity v5-1-street-segments
    (korma/table "v5_1_street_segments"))
  (def v5-1-tables
    (db.util/make-entities "5.1" results-db [:offices
                                             :voter-services
                                             :ballot-measure-contests
                                             :ballot-measure-selections
                                             :ballot-selections
                                             :ballot-styles
                                             :candidates
                                             :candidate-contests
                                             :candidate-selections
                                             :contact-information
                                             :contests
                                             :departments
                                             :elections
                                             :election-administrations
                                             :electoral-districts
                                             :localities
                                             :ordered-contests
                                             :parties
                                             :party-contests
                                             :party-selections
                                             :people
                                             :polling-locations
                                             :precincts
                                             :retention-contests
                                             :schedules
                                             :sources
                                             :states
                                             :street-segments]))
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

(defn find-value-for-simple-path [import-id simple-path]
  (-> (korma/select xml-tree-values
        (korma/fields :value)
        (korma/where {:results_id import-id
                      :simple_path (path->ltree simple-path)}))
      first
      :value))

(defn find-single-xml-tree-value [results-id path]
  (-> (korma/select xml-tree-values
        (korma/fields :value)
        (korma/where {:results_id results-id})
        (korma/where (ltree-match xml-tree-values
                                  :path
                                  path)))
      first
      :value))

(defn start-run [ctx]
  (let [results (korma/insert results
                              (korma/values {:start_time (korma/sqlfn now)
                                             :complete false}))
        import-id (:id results)]
    (log/info "Starting run with import_id:" import-id)
    (assoc ctx :import-id import-id)))

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

(defn get-xml-tree-public-id-data [{:keys [import-id] :as ctx}]
  (let [state (find-value-for-simple-path
               import-id
               "VipObject.State.Name")
        date (find-value-for-simple-path
              import-id
              "VipObject.Election.Date")
        election-type (find-value-for-simple-path
                       import-id
                       "VipObject.Election.ElectionType.Text")]
    {:date date
     :election-type election-type
     :state state
     :import-id import-id}))

(defn get-public-id-data [{:keys [spec-version] :as ctx}]
  (condp = (util/version-without-patch @spec-version)
    "3.0" (get-v3-public-id-data ctx)
    "5.1" (get-xml-tree-public-id-data ctx)
    {}))

(defn generate-public-id [ctx]
  (let [{:keys [date election-type state import-id]} (get-public-id-data ctx)]
    (log/info "Building public id")
    (build-public-id date election-type state import-id)))

(defn generate-election-id [ctx]
  (log/info "Building election id")
  (let [{:keys [date election-type state]} (get-public-id-data ctx)]
    (build-election-id date election-type state)))

(defn store-public-id [ctx]
  (let [id (:import-id ctx)
        public-id (generate-public-id ctx)
        public-id-data (get-public-id-data ctx)]
    (log/info "Storing public id")
    (korma/update results
                  (korma/set-fields {:public_id public-id
                                     :state (:state public-id-data)
                                     :election_type (:election-type public-id-data)
                                     :election_date (:date public-id-data)})
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
      (log/info "Storing election id")
      (save-election-id! election-id)
      (korma/update results
                    (korma/set-fields {:election_id election-id})
                    (korma/where {:id id}))
      (assoc ctx :election-id election-id))
    ctx))

(defn store-spec-version [{:keys [spec-version import-id] :as ctx}]
  (when @spec-version
    (log/info "Storing spec-verion," @spec-version)
    (korma/update results
      (korma/set-fields {:spec_version @spec-version})
      (korma/where {:id import-id})))
  ctx)

(defn analyze-xtv [ctx]
  (log/info "Analyzing xml_tree_values")
  (korma/exec-raw
   (:conn xml-tree-values)
   ["analyze xml_tree_values"])
  ctx)

(defn refresh-materialized-views
  [ctx]
  (log/info "Refreshing materialized views")
  (doseq [view ["v5_dashboard.sources" "v5_dashboard.elections"]]
    (korma/exec-raw
     (:conn xml-tree-values)
     [(str "refresh materialized view " view)]))
  ctx)

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

(defn validation-value
  [{:keys [ctx severity scope identifier error-type error-value]}]
  {:results_id (:import-id ctx)
   :severity (name severity)
   :scope (name scope)
   :identifier (coerce-identifier identifier)
   :error_type (name error-type)
   :error_data (pr-str error-value)})

(defn xml-tree-validation-value
  [{:keys [ctx severity scope identifier error-type error-value]}]
  {:results_id (:import-id ctx)
   :severity (name severity)
   :scope (name scope)
   :error_type (name error-type)
   :error_data (pr-str error-value)
   :path (when-not (= :global identifier) (path->ltree identifier))})

(def statement-parameter-limit 10000)
(def bulk-import (partial db.util/bulk-import statement-parameter-limit))

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
                                (db.util/lazy-select 5000)
                                (map #(assoc % :results_id import-id))
                                (data-spec/coerce-rows columns)))))
          ctx
          db.util/import-entity-names))

(defn columns [table]
  (let [table-name (:table table)]
    (korma/select "information_schema.columns"
      (korma/where {:table_name table-name}))))

(defn column-names [table]
  (map :column_name (columns table)))

(defn lazy-select-xml-tree-values [chunk-size import-id]
  (binding [db/*current-conn* (db/get-connection (:db xml-tree-values))]
    (let [cursor-name (str (gensym "xtv_cursor"))
          done? (atom false)
          close-attempts (atom 0)]
      (korma/exec-raw
                      [(str "DECLARE " cursor-name " NO SCROLL CURSOR "
                            "WITH HOLD FOR "
                            "SELECT * FROM xml_tree_values "
                            "WHERE results_id=" import-id " "
                            "ORDER BY insert_counter ASC;")])
      (letfn [(chunked-rows []
                (when @done?
                  (swap! close-attempts inc))
                (try
                  (do
                    (let [this-chunk (korma/exec-raw
                                      [(str "FETCH " chunk-size
                                            " FROM " cursor-name)]
                                      :results)]
                      (if (seq this-chunk)
                        (do
                          (lazy-cat
                           this-chunk
                           (trampoline chunked-rows)))
                        (do
                          (reset! done? true)
                          (korma/exec-raw
                           [(str "CLOSE " cursor-name)])
                          nil))))
                  (catch java.sql.SQLException e
                    (if (> @close-attempts 30)
                      (do
                        (log/error "Tried to close cursor" cursor-name "too many times")
                        nil)
                      chunked-rows))))]
        (trampoline chunked-rows)))))

(defn lazy-select-fn [chunk-size query-fn]
  (fn [& args]
    (let [cursor-name (str (gensym "lazy_select_cursor"))
          done? (atom false)
          close-attempts (atom 0)
          query (apply query-fn args)]
      (korma/exec-raw
       [(str "DECLARE " cursor-name " NO SCROLL CURSOR "
             "WITH HOLD FOR "
             query)])
      (letfn [(chunked-rows []
                (when @done?
                  (swap! close-attempts inc))
                (try
                  (do
                    (let [this-chunk (korma/exec-raw
                                      [(str "FETCH " chunk-size
                                            " FROM " cursor-name)]
                                      :results)]
                      (if (seq this-chunk)
                        (do
                          (lazy-cat
                           this-chunk
                           (trampoline chunked-rows)))
                        (do
                          (reset! done? true)
                          (korma/exec-raw
                           [(str "CLOSE " cursor-name)])
                          nil))))
                  (catch java.sql.SQLException e
                    (if (> @close-attempts 30)
                      (do
                        (log/error "Tried to close cursor" cursor-name "too many times")
                        nil)
                      chunked-rows))))]
        (trampoline chunked-rows)))))
