(ns vip.data-processor.validation.db
  (:require [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.string :as str]
            [korma.core :as korma]))

(defn has-id? [csv-spec]
  (some #(-> % :name (= "id")) (:columns csv-spec)))

(defn tables-with-ids [ctx]
  (let [csvs-with-id (filter has-id? csv/csv-specs)
        table-names (map :table csvs-with-id)]
    (map (:tables ctx) table-names)))

(defn add-to-seen-ids [seen-ids table]
  (let [ids (korma/select table (korma/fields :id))]
    (reduce (fn [seen-ids {:keys [id]}]
              (update seen-ids id conj (:table table)))
            seen-ids
            ids)))

(defn duplicated-ids [ctx]
  (let [tables (tables-with-ids ctx)
        all-ids (reduce add-to-seen-ids {} tables)]
    (apply dissoc all-ids (for [[k v] all-ids :when (= 1 (count v))] k))))

(defn validate-no-duplicated-ids [ctx]
  (let [dupes (duplicated-ids ctx)]
    (if-not (empty? dupes)
      (assoc-in ctx [:errors "Duplicate IDs"] dupes)
      ctx)))

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

(defn find-dupes-sql [table-name select-fields columns]
  (let [dupe-fields (map dupe-field-select columns)
        select-fields (map (partial str "origin.") select-fields)
        dupe-join-criteria (dupe-join-criteria columns)]
    (str "SELECT " (str/join ", " select-fields)
         " FROM " table-name " origin "
         " INNER JOIN (SELECT " (str/join ", " dupe-fields) ", COUNT(*) AS cnt "
         " FROM " table-name
         " GROUP BY " (str/join ", " columns)
         " HAVING (cnt > 1)) dupes "
         " ON " dupe-join-criteria)))

(defn find-potential-dupes [table]
  (let [columns (sqlite/column-names table)
        columns-without-id (remove (partial = "id") columns)
        select-fields (if (some #{"id"} columns) ["id"] columns-without-id)
        sql (find-dupes-sql (:name table) select-fields columns-without-id)]
    (korma/exec-raw (:db table) [sql] :results)))

(defn validate-no-duplicated-rows-in-table [ctx {:keys [filename table]}]
  (let [table (get-in ctx [:tables table])
        potential-dupes (find-potential-dupes table)]
    (if (seq potential-dupes)
      (assoc-in ctx [:warnings filename :duplicated-rows] potential-dupes)
      ctx)))

(defn validate-no-duplicated-rows [csv-specs]
  (fn [ctx]
    (reduce validate-no-duplicated-rows-in-table ctx csv-specs)))
