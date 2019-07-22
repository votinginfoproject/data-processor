(ns vip.data-processor.db.v5
  "Provides a pair of pipeline steps, `add-feed-indexes` and
  `drop-feed-indexes`. The former should be called in the pipeline
  after the import-id has been set, but before data is added to
  and of the processing tables (ie `xml_tree_values` or
  `xml_tree_validations`). Then the second should be called
  after processing is complete and the data is being deleted."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [vip.data-processor.db.postgres :as postgres]))

(defn values-parent-id-idx
  [id]
  (let [idx-name (str "xml_parent_id_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (parent_with_id) WHERE results_id = %s;" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-idref-validation-idx
  [id]
  (let [idx-name (str "xml_tree_values_idref_validation_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (simple_path) WHERE results_id = '%s' AND simple_path ~ '*{2}.id'::lquery;"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-no-duplicate-ids-idx
  [id]
  (let [idx-name (str "xml_tree_values_no_duplicate_ids_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (results_id DESC NULLS LAST, value COLLATE pg_catalog.\"default\", path)
  WHERE results_id = %s AND path ~ 'VipObject.0.*.id'::lquery;"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-path-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (value) WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-simple-path-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_simple_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (simple_path) WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-date-path-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_date_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (simple_path) WHERE results_id = %s AND simple_path =
 'VipObject.Election.Date'::ltree" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-election-type-path-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_election_type_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path) WHERE results_id = %s AND path ~
 'VipObject.*.Election.*.ElectionType.*.Text.0'::lquery" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-election-type-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_election_type_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (element_type(path) COLLATE pg_catalog.\"default\")
 WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-insert-counter-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_insert_counter_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (insert_counter) WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-state-path-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_state_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path) WHERE results_id = %s AND simple_path =
 'VipObject.State.Name'::ltree" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-i18n-paths-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_i18n_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path) WHERE results_id = %s AND simple_path ~
 'VipObject.*.Text.language'::lquery AND value = 'en'::text" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-i18n-text-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_i18n_text_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path) WHERE results_id = %s AND simple_path ~ 'VipObject.*.Text'::lquery"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-localities-polling-locations-ids-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_localities_polling_locations_idx_"
                      id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path, value COLLATE pg_catalog.\"default\") WHERE results_id = %s AND
 path ~ 'VipObject.*.Locality.*.PollingLocationIds.*'::lquery"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-precincts-polling-locations-ids-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_precincts_polling_locations_idx_"
                      id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (path, value COLLATE pg_catalog.\"default\") WHERE results_id = %s AND
 path ~ 'VipObject.*.Precinct.*.PollingLocationIds.*'::lquery"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn values-result-street-segment-precinct-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_street_segment_precinct_idx_"
                      id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (simple_path, value COLLATE pg_catalog.\"default\")
 WHERE results_id = %s AND simple_path =
 'VipObject.StreetSegment.PrecinctId'::ltree"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

;;New index to improve Locality Stats Performance
(defn values-result-precinct-locality-id-values-idx
  [id]
  (let [idx-name (str "xml_tree_values_result_precinct_locality_id_values_idx_"
                      id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_values USING btree
 (simple_path, value) WHERE results_id = %s AND
 simple_path = 'VipObject.Precinct.LocalityId'::ltree"
                     idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(def values-indexes
  [values-parent-id-idx
   values-idref-validation-idx
   values-no-duplicate-ids-idx
   values-result-path-idx
   values-result-simple-path-idx
   values-result-date-path-idx
   values-result-election-type-path-idx
   values-result-election-type-idx
   values-result-insert-counter-idx
   values-result-state-path-idx
   values-result-i18n-paths-idx
   values-result-i18n-text-idx
   values-result-localities-polling-locations-ids-idx
   values-result-precincts-polling-locations-ids-idx
   values-result-street-segment-precinct-idx
   values-result-precinct-locality-id-values-idx])

(defn validations-result-path-idx
  [id]
  (let [idx-name (str "validations_path_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_validations USING btree
 (path) WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn validations-result-type-idx
  [id]
  (let [idx-name (str "validations_type_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_validations USING btree
 (error_type COLLATE pg_catalog.\"default\") WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(defn validations-result-id-severity-idx
  [id]
  (let [idx-name (str "validations_id_severity_idx_" id)]
    {:name idx-name
     :create (format "CREATE INDEX %s ON xml_tree_validations USING btree
 (severity COLLATE pg_catalog.\"default\") WHERE results_id = %s" idx-name id)
     :drop (format "DROP INDEX %s" idx-name)}))

(def validations-indexes
  [validations-result-path-idx
   validations-result-type-idx
   validations-result-id-severity-idx])

(defn add-feed-indexes
  "Creates a suite of feed specific indexes for processing"
  [{:keys [import-id] :as ctx}]
  (let [indexes (map #(% import-id) (concat values-indexes
                                            validations-indexes))]
    (log/info "Creating feed indexes " (pr-str (mapv :name indexes)))
    (jdbc/db-do-commands (postgres/db-spec) (mapv :create indexes))
    (assoc ctx :drop-feed-indexes (mapv :drop indexes))))

(defn drop-feed-indexes
  "Drops the suite of feed specific indexes, called post processing"
  [{:keys [drop-feed-indexes keep-feed-on-complete?] :as ctx}]
  (when-not keep-feed-on-complete?
    (log/info "Dropping feed indexes")
    (jdbc/db-do-commands (postgres/db-spec) drop-feed-indexes))
  ctx)
