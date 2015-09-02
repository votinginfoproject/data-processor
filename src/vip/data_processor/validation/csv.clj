(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [korma.core :as korma]
            [korma.db :as db]
            [vip.data-processor.db.util :as util]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.db.sqlite :as sqlite]))

(def csv-filenames (set (map :filename data-spec/data-specs)))

(defn file-name [file]
  (.getName file))

(defn good-filename? [file]
  (let [filename (file-name file)]
    (contains? csv-filenames filename)))

(defn remove-bad-filenames [ctx]
  (let [input (:input ctx)
        {good-files true bad-files false} (group-by good-filename? input)]
    (if (seq bad-files)
      (let [bad-filenames (->> bad-files (map file-name) sort)
            bad-file-list (apply str (interpose ", " bad-filenames))]
        (-> ctx
            (update-in [:warnings :import :global :bad-filenames]
                       conj bad-file-list)
            (assoc :input good-files)))
      ctx)))

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (.getName %)))
       first))

(defn retry-chunk-without-dupe-ids
  "If inserting a chunk has failed, retry trying to remove duplicate ids"
  [ctx sql-table chunk-values]
  (binding [db/*current-conn* (db/get-connection (:db sql-table))]
    (db/transaction
     (let [table (-> sql-table :name keyword)
           existing-ids (->> (korma/select sql-table
                                           (korma/fields :id)
                                           (korma/where {:id [in (map #(Integer/parseInt (get % "id")) chunk-values)]}))
                             (map (comp str :id))
                             set)
           local-dupe-ids (->> chunk-values
                               (map #(get % "id"))
                               frequencies
                               (filter (fn [[k v]] (> v 1)))
                               (map first)
                               set)
           chunk-without-dupe-ids (remove (fn [{:strs [id]}]
                                            (or (existing-ids id)
                                                (local-dupe-ids id)))
                                          chunk-values)]
       (korma/insert sql-table (korma/values chunk-without-dupe-ids))
       (reduce (fn [ctx dupe-id]
                 (assoc-in ctx [:fatal table dupe-id :duplicate-ids]
                           ["Duplicate id"]))
               ctx (set/union existing-ids local-dupe-ids))))))

(defn bulk-import-and-validate-csv
  "Bulk importing and CSV validation is done in one go, so that it can
  be done without holding the entire file in memory at once."
  [ctx {:keys [filename table columns] :as data-spec}]
  (if-let [file-to-load (find-input-file ctx filename)]
    (do
      (log/info "Loading" filename)
      (with-open [in-file (io/reader file-to-load :encoding "UTF-8")]
        (let [headers (-> in-file
                          .readLine
                          csv/read-csv
                          first)
              headers-count (count headers)
              sql-table (get-in ctx [:tables table])
              column-names (map :name columns)
              required-header-names (->> columns
                                         (filter :required)
                                         (map :name))
              extraneous-headers (seq (set/difference (set headers) (set column-names)))
              transforms (apply comp (data-spec/translation-fns columns))
              format-rules (data-spec/create-format-rules filename columns)
              ctx (if extraneous-headers
                    (assoc-in ctx [:warnings table :global :extraneous-headers]
                              extraneous-headers)
                    ctx)
              line-number (atom 1)]
          (if (empty? (set/intersection (set headers) (set column-names)))
            (assoc-in ctx [:critical table :global :no-header] ["No header row"])
            (if-let [missing-headers (seq (set/difference (set required-header-names) (set headers)))]
              (assoc-in ctx [:critical table :global :missing-headers] missing-headers)
              (reduce (fn [ctx chunk]
                        (if (seq chunk)
                          (let [ctx (reduce (fn [ctx row]
                                              (swap! line-number inc)
                                              (let [row-map (zipmap headers row)
                                                    row-count (count row)
                                                    ctx (data-spec/apply-format-rules format-rules ctx row-map @line-number)]
                                                (if (= headers-count row-count)
                                                  ctx
                                                  (assoc-in ctx [:critical table @line-number :number-of-values]
                                                            [(str "Expected " headers-count
                                                                  " values, found " row-count)]))))
                                            ctx chunk)
                                chunk-values (map (comp transforms #(select-keys % column-names) (partial zipmap headers)) chunk)]
                            (try
                              (korma/insert sql-table (korma/values chunk-values))
                              ctx
                              (catch java.sql.SQLException e
                                (let [message (.getMessage e)]
                                  (if (re-find #"UNIQUE constraint failed: (\w+).id" message)
                                    (retry-chunk-without-dupe-ids ctx sql-table chunk-values)
                                    (assoc-in ctx [:fatal table @line-number :unknown-sql-error]
                                              [message]))))))
                          ctx))
                      ctx (util/chunk-rows (csv/read-csv in-file)
                                           sqlite/statement-parameter-limit)))))))
    ctx))

(defn load-csvs [ctx]
  (reduce bulk-import-and-validate-csv ctx (:data-specs ctx)))

(defn add-report-on-missing-file-fn
  "Generates a validation function generator that takes a filename and
  associates a report-type on the context if the filename is missing."
  [report-type]
  (fn [filename]
    (let [data-spec (first (filter #(= filename (:filename %)) data-spec/data-specs))
          table (:table data-spec)]
      (fn [ctx]
        (if (find-input-file ctx filename)
          ctx
          (assoc-in ctx [report-type table :global :missing-csv] [(str filename " is missing")]))))))

(def ^{:doc "Generates a validation function that adds a warning when
  the given filename is missing from the input"}
  warn-on-missing-file
  (add-report-on-missing-file-fn :warnings))

(def ^{:doc "Generates a validation function that adds an error when
  the given filename is missing from the input"}
  error-on-missing-file
  (add-report-on-missing-file-fn :errors))
