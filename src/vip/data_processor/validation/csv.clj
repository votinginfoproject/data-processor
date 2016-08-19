(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [korma.core :as korma]
            [korma.db :as db]
            [vip.data-processor.util :as util]
            [vip.data-processor.db.util :as db.util]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [vip.data-processor.output.tree-xml :as tree-xml]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.transformer :as v5-1-transformers]))

(defn csv-filenames [data-specs]
  (set (map :filename data-specs)))

(defn file-name [file]
  (.getName file))

(defn good-filename? [data-specs file]
  (let [filename (str/lower-case (file-name file))]
    (contains? (csv-filenames data-specs) filename)))

(defn-traced remove-bad-filenames [ctx]
  (let [input (:input ctx)
        {good-files true bad-files false} (group-by (partial good-filename? (:data-specs ctx)) input)]
    (if (seq bad-files)
      (let [bad-filenames (->> bad-files (map file-name) sort)
            bad-file-list (apply str (interpose ", " bad-filenames))]
        (-> ctx
            (update-in [:warnings :import :global :bad-filenames]
                       conj bad-file-list)
            (assoc :input good-files)))
      ctx)))

(defn csv-error-path [filename line-number]
  (str "CSVError.0."
       (-> filename
           (str/split #"\.")
           first)
       "." line-number))

(defn read-one-line
  "Reads one line at a time from a reader and parses it as a CSV
  string. Does not close the reader at the end, that's your job."
  [reader]
  (->> reader
       .readLine
       csv/read-csv
       first))

(defn-traced bulk-import-and-validate-csv
  "Bulk importing and CSV validation is done in one go, so that it can
  be done without holding the entire file in memory at once."
  [ctx {:keys [filename table columns] :as data-spec}]
  (if-let [file-to-load (util/find-input-file ctx filename)]
    (do
      (log/info "Loading" filename)
      (try
        (with-open [in-file (util/bom-safe-reader file-to-load :encoding "UTF-8")]
          (let [headers (->> in-file
                             read-one-line
                             (map #(str/replace % #"\W" ""))
                             (map str/lower-case))
                headers-count (count headers)
                sql-table (get-in ctx [:tables table])
                column-names (map :name columns)
                required-header-names (->> columns
                                           (filter :required)
                                           (map :name)
                                           (map str/lower-case))
                extraneous-headers (seq (set/difference (set headers) (set column-names)))
                transforms (apply comp (data-spec/translation-fns columns))
                format-rules (data-spec/create-format-rules (:data-specs ctx) filename columns)
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
                                                    (let [identifier (if (= "3.0" (:spec-version ctx))
                                                                       @line-number
                                                                       (csv-error-path filename @line-number))]
                                                      (assoc-in ctx [:critical table identifier :number-of-values]
                                                                [(str "Expected " headers-count
                                                                      " values, found " row-count)])))))
                                              ctx chunk)
                                  chunk-values (map (comp transforms #(select-keys % column-names) (partial zipmap headers)) chunk)
                                  chunk-values (if (= "postgresql" (get-in sql-table [:db :options :subprotocol]))
                                                 (->> chunk-values
                                                      (data-spec/coerce-rows columns)
                                                      (map #(assoc % "results_id" (:import-id ctx))))
                                                 chunk-values)]
                              (try
                                (korma/insert sql-table (korma/values chunk-values))
                                ctx
                                (catch java.sql.SQLException e
                                  (let [message (.getMessage e)]
                                    (if (re-find #"UNIQUE constraint failed: (\w+).id" message)
                                      (db.util/retry-chunk-without-dupe-ids ctx sql-table chunk-values)
                                      (let [identifier (if (= "3.0" (:spec-version ctx))
                                                         @line-number
                                                         (csv-error-path filename @line-number))]
                                        (assoc-in ctx [:fatal (:name sql-table) identifier :unknown-sql-error]
                                                  [message])))))))
                            ctx))
                        ctx (db.util/chunk-rows (csv/read-csv in-file)
                                                sqlite/statement-parameter-limit))))))
        (catch java.lang.Exception e
          (update-in ctx [:fatal filename :global :csv-error] conj (.getMessage e)))))
    ctx))

(defn-traced load-csvs [ctx]
  (reduce bulk-import-and-validate-csv ctx (:data-specs ctx)))

(defn error-on-missing-files
  "Add errors for any file with the :required key in the data-spec."
  [{:keys [data-specs] :as ctx}]
  (let [required-files (filter :required data-specs)]
    (reduce (fn [ctx {:keys [filename required table]}]
              (if (util/find-input-file ctx filename)
                ctx
                (assoc-in ctx
                          [required table :global :missing-csv]
                          [(str filename " is missing")])))
            ctx required-files)))

(defn determine-spec-version [ctx]
  (if-let [source-file (util/find-input-file ctx "source.txt")]
    (with-open [reader (util/bom-safe-reader source-file :encoding "UTF-8")]
      (let [headers (read-one-line reader)
            line1 (read-one-line reader)
            csv-map (zipmap headers line1)
            version (get csv-map "version" "3.0")]
        (-> ctx
            (assoc :spec-version version)
            (assoc :data-specs (get data-spec/version-specs version)))))
    (assoc-in ctx [:fatal :sources :global :missing-csv]
              ["source.txt is missing"])))

(defn unsupported-version [{:keys [spec-version] :as ctx}]
  (assoc ctx :stop (str "Unsupported CSV version: " spec-version)))

(def version-pipelines
  {"3.0" [sqlite/attach-sqlite-db
          (csv-files/validate-dependencies csv-files/v3-0-file-dependencies)
          load-csvs]
   "5.1" (concat [(fn [ctx] (assoc ctx :tables postgres/v5-1-tables))
                  (fn [ctx] (assoc ctx :ltree-index 0))
                  load-csvs]
                 v5-1-transformers/transformers
                 tree-xml/pipeline)})

(defn branch-on-spec-version [{:keys [spec-version] :as ctx}]
  (if-let [pipeline (get version-pipelines spec-version)]
    (update ctx :pipeline (partial concat pipeline))
    (unsupported-version ctx)))
