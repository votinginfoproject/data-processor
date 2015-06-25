(ns vip.data-processor.db.statistics
  (:require [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.util :as util]))

(defn data-specs-with-stats [data-specs]
  (filter :stats data-specs))

(defn count-table [sqlite-table]
  (-> sqlite-table
      (korma/select
       (korma/aggregate (count "*") :cnt))
      first
      :cnt))

(defn error-count [table-name {:keys [warnings errors critical fatal]}]
  (transduce (comp (map table-name)
                   (remove nil?)
                   (map util/flatten-keys)
                   (mapcat vals)
                   (map count))
             + 0
             [warnings errors critical fatal]))

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

;;; TODO: fix this
(defn stats-map [ctx]
  (let [stats (stats ctx)]
    (->> stats
         (map (fn [[table {:keys [count error-count complete]}]]
                (let [table-prefix (-> table
                                       name
                                       (str/replace "-" "_"))]
                  {(keyword (str table-prefix "_count")) count
                   (keyword (str table-prefix "_error_count")) error-count
                   (keyword (str table-prefix "_completion")) complete})))
         (reduce merge))))
