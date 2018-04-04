(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [korma.core :as korma]
            [korma.db :as db]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.db.translations.transformer :as v5-1-transformers]
            [vip.data-processor.db.util :as db.util]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.output.tree-xml :as tree-xml]
            [vip.data-processor.util :as util]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]))

(defn csv-filenames [data-specs]
  (set (map :filename data-specs)))

(defn file-name [file]
  (.getName file))

(defn good-filename? [data-specs file]
  (let [filename (str/lower-case (file-name file))]
    (contains? (csv-filenames data-specs) filename)))

(defn-traced remove-bad-filenames [ctx]
  (let [input (:input ctx)
        {good-files true
         bad-files false} (group-by (partial good-filename? (:data-specs ctx)) input)]
    (if (seq bad-files)
      (let [bad-filenames (->> bad-files (map file-name) sort)
            bad-file-list (apply str (interpose ", " bad-filenames))]
        (-> ctx
            (errors/add-errors
             :warnings :import :global :bad-filenames
             bad-file-list)
            (assoc :input good-files)))
      ctx)))

(defn csv-error-path
  [filename line-number]
  (str "CSVError.0."
       (-> filename (str/split #"\.") first)
       "." line-number))

(defn process-row
  "Uses the supplied `format-rules` to process a given row. If there
  is a mismatch in the size of the columns in the row and the columns
  specified by the header, records an error. Increments the
  line-number and returns the context `ctx` with the formatted row
  merged in."
  [{:keys [filename table] :as data-spec}
   {:keys [line-number headers headers-count format-rules] :as file-ctx}
   ctx row]
  (swap! line-number inc)
  (let [row-map (zipmap headers row)
        row-count (count row)
        ctx (data-spec/apply-format-rules format-rules ctx row-map @line-number)]
    (if (= headers-count row-count)
      ctx
      (let [identifier (if (= "3.0" @(:spec-version ctx))
                         @line-number
                         (csv-error-path filename @line-number))]
        (errors/add-errors
         ctx :critical table identifier :number-of-values
         (str "Expected " headers-count
              " values, found " row-count))))))

(defn maybe-coerce-to-db-type
  [{:keys [import-id]} sql-table columns chunk-values]
  (if (= "postgresql" (get-in sql-table [:db :options :subprotocol]))
    (->> chunk-values
         (data-spec/coerce-rows columns)
         (map #(assoc % "results_id" import-id)))
    chunk-values))

(defn process-row-chunks
  "Processes all the map rows in a chunk via `process-row` with the
  supplied format-rules (see `data-spec/create-format-rules` in
  `do-bulk-import-and-validate-csv`) and translations (see
  `data-spec/translation-fns` in
  `do-bulk-import-and-validate-csv`). Inserts the row into the
  database, retries any UNIQUE id constraint-failing SQLExceptions
  removing ids, and marks others as errors. Returns the context
  `ctx`. If the supplied chunk is empty, simply returns the context
  as-is."
  [{:keys [columns filename] :as data-spec}
   {:keys [row-translation-fn column-names headers sql-table line-number] :as file-ctx}
   ctx chunk]
  (if (seq chunk)
    (let [ctx' (reduce (partial process-row data-spec file-ctx) ctx chunk)
          chunk-values (->> chunk
                            (map row-translation-fn)
                            (maybe-coerce-to-db-type ctx' sql-table columns))]
      (try
        (korma/insert sql-table (korma/values chunk-values))
        ctx'
        (catch java.sql.SQLException e
          (let [message (.getMessage e)]
            (if (re-find #"UNIQUE constraint failed: (\w+).id" message)
              (db.util/retry-chunk-without-dupe-ids ctx' sql-table chunk-values)
              (let [identifier (if (= "3.0" @(:spec-version ctx'))
                                 @line-number
                                 (csv-error-path filename @line-number))]
                (errors/add-errors
                 ctx' :fatal (:name sql-table) identifier
                 :unknown-sql-error message)))))))
    ctx))

(defn chunk-rows-and-process
  "Converts the file into map rows using clojure.data.csv, chunks them
  up into partitions so that we don't go over our statement parameter
  limit, and then processes all the chunks using
  `process-row-chunks`. Returns the context `ctx`. If no headers are
  present reports an error and returns the context as-is."
  [ctx
   {:keys [table] :as data-spec}
   {:keys [in-file headers required-header-names] :as file-ctx}]
  (if-let [missing-headers (seq (set/difference (set required-header-names)
                                                (set headers)))]
    (apply
     errors/add-errors
     ctx :critical table :global :missing-headers missing-headers)
    (->> (db.util/chunk-rows
          (csv/read-csv in-file) sqlite/statement-parameter-limit)
         (reduce (partial process-row-chunks data-spec file-ctx) ctx))))

(defn read-one-line
  "Reads one line at a time from a reader and parses it as a CSV
  string. Does NOT close the reader at the end."
  [reader]
  (->> reader .readLine csv/read-csv first))

(defn warn-if-extraneous-headers
  "Records an error if headers are present in the file that do not
  correspond to any in the data-spec (`column-names`)."
  [ctx {:keys [table] :as data-spec} headers column-names]
  (let [extraneous-headers (seq (set/difference (set headers) (set column-names)))]
    (if extraneous-headers
      (apply errors/add-errors
             ctx :warnings table :global :extraneous-headers
             extraneous-headers)
      ctx)))

(defn file-handle->string-keys
  "Given a file handle, returns a collection of cleaned (of non word
  characters), lower-cased string keys for the first line, assumed to
  be the header row."
  [file-handle]
  (->> file-handle
       read-one-line
       (map #(str/replace % #"\W" ""))
       (map str/lower-case)))

(defn required-header-names
  "Given a collection of column maps, returns cleaned (of non word
  characters), lower-cased string names (:name) of all the columns
  which are required (:required)."
  [columns]
  (-> (comp (filter :required)
            (map :name)
            (map str/lower-case))
      (eduction columns)))

(defn make-row-translation-fn
  "Takes columns, column names, and headers, and composes a function
  that takes a chunk of rows and applies each data-spec translation
  function for a given row as provided by `data-spec/translation-fns`."
  [columns column-names headers]
  (comp
   (apply comp (data-spec/translation-fns columns))
   #(select-keys % column-names)
   (partial zipmap headers)))

(defn do-bulk-import-and-validate-csv
  "Prepares all the necessary formatting and translation functions
  along with other useful meta-data and initiates the insertion and
  preliminary validation of CSV data using the chunking mechanism in
  vip.data-processor.db.util/chunk-rows. Returns the context `ctx`
  having performed all necessary database insertions, error reporting,
  and other preliminary validation.

  Records an error if there are extraneous header columns or no
  overlap between the file headers and the data-spec column-names."
  [{:keys [data-specs tables] :as ctx}
   {:keys [filename table columns] :as data-spec}
   in-file]
  (let [headers (file-handle->string-keys in-file)
        column-names (map :name columns)
        ctx (warn-if-extraneous-headers ctx data-spec headers column-names)]
    (if (empty? (set/intersection (set headers) (set column-names)))
      (errors/add-errors ctx :critical table :global :no-header "No header row")
      ;; Local file context needed for any given row during processing
      (->> {:in-file in-file
            :sql-table (get tables table)
            :headers headers
            :headers-count (count headers)
            :column-names column-names
            :required-header-names (required-header-names columns)
            :line-number (atom 1)
            :format-rules
            (data-spec/create-format-rules data-specs filename columns)
            :row-translation-fn
            (make-row-translation-fn columns column-names headers)}
            (chunk-rows-and-process ctx data-spec)))))

(defn-traced bulk-import-and-validate-csv
  "Bulk importing and CSV validation is done in one go, so that it can
  be done without holding the entire file in memory at once."
  [ctx {:keys [filename] :as data-spec}]
  (if-let [file-to-load (util/find-input-file ctx filename)]
    (do
      (log/info "Loading" filename)
      (try
        (with-open [in-file (util/bom-safe-reader file-to-load :encoding "UTF-8")]
          (do-bulk-import-and-validate-csv ctx data-spec in-file))
        (catch java.lang.Exception e
          (errors/add-errors ctx :fatal filename :global :csv-error (.getMessage e)))))
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
                (errors/add-errors ctx
                                   required table :global :missing-csv
                                   (str filename " is missing"))))
            ctx required-files)))

(defn determine-spec-version [ctx]
  (if-let [source-file (util/find-input-file ctx "source.txt")]
    (with-open [reader (util/bom-safe-reader source-file :encoding "UTF-8")]
      (let [headers (read-one-line reader)
            line1 (read-one-line reader)
            csv-map (zipmap headers line1)
            version (get csv-map "version" "3.0")]
        (-> ctx
            (update :spec-version (fn [spec-version]
                                    (reset! spec-version version)
                                    spec-version))
            (assoc :data-specs (get data-spec/version-specs
                                    (util/version-without-patch version))))))
    (errors/add-errors ctx :fatal :sources :global :missing-csv
                       "source.txt is missing")))

(defn unsupported-version [{:keys [spec-version] :as ctx}]
  (assoc ctx :stop (str "Unsupported CSV version: " (pr-str @spec-version))))

(def version-pipelines
  {"3.0" [sqlite/attach-sqlite-db
          process/process-v3-validations
          (csv-files/validate-dependencies csv-files/v3-0-file-dependencies)
          load-csvs]
   "5.1" (concat [(fn [ctx] (assoc ctx :tables postgres/v5-1-tables))
                  (fn [ctx] (assoc ctx :ltree-index 0))
                  process/process-v5-validations
                  load-csvs]
                 v5-1-transformers/transformers
                 tree-xml/pipeline)})

(defn branch-on-spec-version [{:keys [spec-version] :as ctx}]
  (if-let [pipeline (get version-pipelines
                         (util/version-without-patch @spec-version))]
    (update ctx :pipeline (partial concat pipeline))
    (unsupported-version ctx)))
