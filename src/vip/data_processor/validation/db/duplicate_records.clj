(ns vip.data-processor.validation.db.duplicate-records
  (:require [vip.data-processor.db.sqlite :as sqlite]
            [clojure.string :as str]
            [korma.core :as korma]))

(defn columns-without-id [table]
  (remove (partial = "id") (sqlite/column-names table)))

(defn dupe-column-name [column]
  (str "dupes." column))

(defn if-null [column-name]
  (str "IFNULL(" column-name ", \"NULL_VALUE\")"))

(defn dupe-field-select [column-name]
  (str (if-null column-name) " AS " column-name))

(defn dupe-join-criterion [column-name]
  (str "dupes." column-name " = " (if-null (str "origin." column-name))))

(defn dupe-join-criteria [columns]
  (str/join " AND "
            (map dupe-join-criterion columns)))

(defn find-dupes-sql [table-name columns]
  (let [dupe-fields (map dupe-field-select columns)
        dupe-join-criteria (dupe-join-criteria columns)]
    (str "SELECT origin.* "
         " FROM " table-name " origin "
         " INNER JOIN (SELECT " (str/join ", " dupe-fields) ", COUNT(*) AS cnt "
         " FROM " table-name
         " GROUP BY " (str/join ", " columns)
         " HAVING (cnt > 1)) dupes "
         " ON " dupe-join-criteria)))

(defn find-potential-dupes [table]
  (let [columns (sqlite/column-names table)
        columns-without-id (remove (partial = "id") columns)
        sql (find-dupes-sql (:name table) columns-without-id)]
    (korma/exec-raw (:db table) [sql] :results)))

(defn validate-no-duplicated-rows-in-table [ctx {:keys [filename table]}]
  (let [table (get-in ctx [:tables table])
        potential-dupes (find-potential-dupes table)]
    (if (seq potential-dupes)
      (assoc-in ctx [:warnings filename :duplicated-rows] potential-dupes)
      ctx)))
