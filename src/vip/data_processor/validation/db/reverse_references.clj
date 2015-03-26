(ns vip.data-processor.validation.db.reverse-references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]
            [korma.sql.engine :as eng]))

(defn find-referencing-column [table-name csv-spec]
  (->> csv-spec
       :columns
       (filter #(= table-name (:references %)))
       first))

(defn find-referencing-tables [table-name csv-specs]
  (into {}
        (for [spec csv-specs
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

(defn find-unreferenced-rows [tables table-id references-map]
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

(defn find-all-referenced-tables [csv-specs]
  (into {} (for [csv-spec csv-specs
                 :let [table (:table csv-spec)
                       references (find-referencing-tables table csv-specs)]
                 :when (seq references)]
             [table references])))

(defn validate-no-unreferenced-rows-for-table [ctx [table-id references-map]]
  (let [tables (:tables ctx)
        unreferenced-rows (find-unreferenced-rows tables table-id references-map)]
    (if (seq unreferenced-rows)
      (assoc-in ctx [:warnings table-id "Unreferenced records"] unreferenced-rows)
      ctx)))
