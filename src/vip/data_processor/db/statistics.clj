(ns vip.data-processor.db.statistics
  (:require [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as psql]))

(defn data-specs-with-stats [data-specs]
  (filter :stats data-specs))

(defn count-table [sqlite-table]
  (-> sqlite-table
      (korma/select
       (korma/aggregate (count "*") :cnt))
      first
      :cnt))

(defn error-count [table-name {:keys [import-id]}]
  (-> psql/validations
   (korma/select
    (korma/aggregate (count "*") :cnt)
    (korma/where {:results_id import-id
                  :scope (name table-name)}))
   first
   :cnt))

(defn complete [row-count error-count]
  (cond
    (> error-count row-count) 0
    (= row-count 0) 100
    :else (-> (/ (- row-count error-count)
                 row-count)
              (* 100)
              float
              Math/round)))

(defn stats-for-table [ctx data-spec]
  (let [table-key (:table data-spec)
        rows (count-table (get-in ctx [:tables table-key]))
        errors (error-count table-key ctx)]
    {:count rows
     :error-count errors
     :complete (complete rows errors)}))

(defn stats [ctx]
  (let [specs (data-specs-with-stats (:data-specs ctx))]
    (into {} (map (juxt :table (partial stats-for-table ctx)) specs))))

(defn stats-map [{:keys [import-id] :as ctx}]
  (let [stats (stats import-id)]
    (->> stats
         (map (fn [[table {:keys [count error-count complete]}]]
                (let [table-prefix (-> table
                                       name
                                       (str/replace "-" "_"))]
                  {(keyword (str table-prefix "_count")) count
                   (keyword (str table-prefix "_error_count")) error-count
                   (keyword (str table-prefix "_completion")) complete})))
         (reduce merge))))

(defn store-stats [{:keys [import-id] :as ctx}]
  (korma/insert psql/statistics
    (korma/values (assoc (stats-map import-id) :results_id import-id)))
  ctx)
