(ns vip.data-processor.db.tree-statistics
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [korma.core :as korma]
            [vip.data-processor.util :as util]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as t-util]))

(defn reported-elements []
  (->> postgres/v5-statistics
       postgres/column-names
       (filter #(str/ends-with? % "_count"))
       (map #(str/replace % #"_count" ""))
       (map t-util/column->xml-elment)
       (remove #(str/starts-with? % "PollingLocation"))))

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
  FULL OUTER JOIN errors ON errors.element_type = values.element_type
  WHERE values.element_type IS NOT NULL;")))

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
  [{:keys [import-id]}]
  (->> (korma/exec-raw [(error-query) [import-id import-id]] :results)
       (map stats-map)
       (reduce merge)))

(defn store-tree-stats
  [{:keys [import-id] :as ctx}]
  (log/info "Building basic feed stats")
  (korma/insert postgres/v5-statistics
    (korma/values
     (assoc (stats ctx) :results_id import-id)))
  (log/info "Building locality stats")
  (korma/exec-raw
   ["select * from v5_dashboard.feed_localities(?)" [import-id]])
  ctx)
