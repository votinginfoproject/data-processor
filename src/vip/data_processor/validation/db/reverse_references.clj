(ns vip.data-processor.validation.db.reverse-references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]
            [korma.sql.engine :as eng]))

(defn find-referencing-column [table-name data-spec]
  (->> data-spec
       :columns
       (filter #(= table-name (:references %)))
       first))

(defn find-referencing-tables
  "Returns a map from table keys to column names of all tables in the
  data-spec that contain references to `table-name`."
  [table-name data-specs]
  (into {}
        (for [spec data-specs
              :let [referencing-column (:name (find-referencing-column table-name spec))
                    table (:table spec)]
              :when referencing-column]
          [table referencing-column])))

(defn join-clause [from-table from-column to-table]
  (let [from-column (util/column-name from-table from-column)
        to-column (util/column-name to-table "id")]
    (list '= from-column to-column)))

(defn where-clause-part [table column]
  (let [column (util/column-name table column)]
    (list '= column nil)))

(defn find-unreferenced-rows
  "Find all rows in the database that can be referenced by another
  table but which are not. For example, there are a few tables that
  have a `locality_id` column. This finds any rows in `localities`
  that are never referenced by any of those tables' `locality_id`
  column.

  * `tables` a map with korma tables as values
  * `table-id` is the key for the table we're checking references for
  * `references-map` is a map describing how tables reference each
    other, in the form returned from `find-referencing-tables`."
  [tables table-id references-map]
  (let [basis (table-id tables)
        where-clause-parts (map (fn [[table-id column]]
                                  (let [table-name (:name (table-id tables))]
                                    (where-clause-part table-name column)))
                                references-map)
        where-clause (concat '(and) where-clause-parts)
        join-clauses (map (fn [[table-id column]]
                            (let [table-name (:name (table-id tables))]
                              [table-id (join-clause table-name column (:name basis))]))
                          references-map)
        query (-> (korma/select* (table-id tables))
                  (korma/where* (eng/pred-map (eval (eng/parse-where where-clause)))))
        query (reduce (fn [query [table-id join-clause]]
                        (korma/join* query :left (table-id tables)
                                     (eng/pred-map (eval (eng/parse-where join-clause)))))
                      query
                      join-clauses)]
    (korma/exec query)))

(defn find-all-referenced-tables [data-specs]
  (into {} (for [data-spec data-specs
                 :let [table (:table data-spec)
                       references (find-referencing-tables table data-specs)]
                 :when (seq references)]
             [table references])))

(defn validate-no-unreferenced-rows-for-table [ctx [table-id references-map]]
  (let [tables (:tables ctx)
        unreferenced-rows (find-unreferenced-rows tables table-id references-map)]
    (if (seq unreferenced-rows)
      (let [filename (->> ctx
                          :data-specs
                          (filter #(= table-id (:table %)))
                          first
                          :filename)]
        (assoc-in ctx [:warnings filename :unreferenced-rows] unreferenced-rows))
      ctx)))
