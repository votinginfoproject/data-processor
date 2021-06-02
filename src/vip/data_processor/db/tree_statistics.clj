(ns vip.data-processor.db.tree-statistics
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [korma.core :as korma]
            [korma.db :as korma.db]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as t-util]))

(defn reported-elements
  "Function to generate elements to use in the error-query; starting with the
   column names from the v5_statistics table we have to manipulate the headings
   where necessary to remove polling locations (i.e. starts-with PollingLocation)
   and ev_polling_locations and db_polling_locations
   (i.e. ends with PollingLocation) as their stats are all calculated by the
   v5_statistics.polling_locations_by_type function"
  []
  (->> postgres/v5-statistics
       postgres/column-names
       (filter #(str/ends-with? % "_count"))
       (map #(str/replace % #"_count" ""))
       (map t-util/column->xml-elment)
       (remove #(str/starts-with? % "PollingLocation"))
       (remove #(str/ends-with? % "PollingLocation"))))

(defn error-query []
  (let [element-paths (str/join "|" (reported-elements))]
    (str
     "WITH errors AS (SELECT element_type(errors.path) AS element_type,
                         COUNT(errors.severity) AS error_count
                  FROM results r
                  LEFT JOIN xml_tree_validations errors ON r.id = errors.results_id
                  WHERE r.id = ?
                    AND errors.path IS NOT NULL
                  GROUP BY element_type),
       values AS (SELECT element_type(values.path) AS element_type,
                         COUNT(DISTINCT countable_path(values.path)) as value_count
                  FROM results r
                  LEFT JOIN xml_tree_values values ON r.id = values.results_id
                  WHERE r.id = ?
                    AND values.path ~ 'VipObject.0." element-paths ".*'
                    AND values.path IS NOT NULL
                  GROUP BY element_type)
  SELECT coalesce(errors.element_type, values.element_type) AS element,
         coalesce(errors.error_count, 0) as error_count,
         coalesce(values.value_count, 0) as count
  FROM values
  FULL OUTER JOIN errors ON errors.element_type = values.element_type;")))

(def camel-case-splitter #"(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")

(defn camel->snake [s]
  (->> (str/split s camel-case-splitter)
       (str/join "_")
       (str/lower-case)))

(defn complete [row-count error-count]
  (cond
    (> error-count row-count) 0
    (= row-count 0) 100
    :else (-> (/ (- row-count error-count)
                 row-count)
              (* 100)
              float
              Math/round)))

(defn stats-map
  [{:keys [element error_count count]}]
  (let [completion (complete count error_count)]
    {(keyword (str (camel->snake element) "_count")) count
     (keyword (str (camel->snake element) "_errors")) error_count
     (keyword (str (camel->snake element) "_completion")) completion}))

(defn stats
  [import-id]
  (->> (korma/exec-raw [(error-query) [import-id import-id]] :results)
       (map stats-map)
       (reduce merge)))

(defn prepare-temp-data-for-locality-stats
  "Adds steet segment validation data to processing table for faster
   processing, only used for the locality stats computations and then
   deleted"
  [import-id]
  (log/info "Saving temp data for locality stats")
  (korma/exec-raw
   ["insert into v5_street_segment_validations
       select * from xml_tree_validations
       where results_id = ?
             and path ~ 'VipObject.0.StreetSegment.*'" [import-id]]))

(defn cleanup-temp-data-for-locality-stats
  "Deletes street segment validation data from processing table"
  [import-id]
  (log/info "Deleting temp data for locality stats")
  (korma/exec-raw
   ["delete from v5_street_segment_validations where results_id = ?"
    [import-id]]))

(defn locality-ids
  "Gets all the locality ids for the feed."
  [import-id]
  (->> (korma/select postgres/xml-tree-values
         (korma/fields :value)
         (korma/where {:simple_path
                       (postgres/path->ltree "VipObject.Locality.id")})
         (korma/where {:results_id import-id}))
       (map :value)))

(defn compute-locality-stats
  "Runs through each locality and computes the stats for it, logging along
   progress along the way."
  [import-id]
  (log/info "Computing locality stats")
  (let [locality-ids (locality-ids import-id)
        loc-count (count locality-ids)]
    (doall (map-indexed
            (fn [idx locality-id]
              (log/info "Processing locality" (inc idx) "of" loc-count
                        "with id" locality-id)
              (korma/exec-raw
               ["insert into v5_dashboard.localities
                              select * from v5_dashboard.locality_stats(?, ?)"
                [import-id locality-id]]))
            locality-ids))))

(defn locality-stats [import-id]
  (prepare-temp-data-for-locality-stats import-id)
  (compute-locality-stats import-id)
  (cleanup-temp-data-for-locality-stats import-id))

(defn store-tree-stats
  [import-id]
  (log/info "Building basic feed stats")
  (korma/insert postgres/v5-statistics
    (korma/values
     (assoc (stats import-id) :results_id import-id)))
  (log/info "Getting additional feed stats")
  (korma/exec-raw
   ["select * from v5_dashboard.polling_locations_by_type(?)" [import-id]])
  (locality-stats import-id))
