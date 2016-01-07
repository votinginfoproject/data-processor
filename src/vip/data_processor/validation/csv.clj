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
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.db.sqlite :as sqlite]))

(defn csv-filenames [data-specs]
  (set (map :filename data-specs)))

(defn file-name [file]
  (.getName file))

(defn good-filename? [data-specs file]
  (let [filename (clojure.string/lower-case (file-name file))]
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

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (clojure.string/lower-case (.getName %))))
       first))

(defn-traced bulk-import-and-validate-csv
  "Bulk importing and CSV validation is done in one go, so that it can
  be done without holding the entire file in memory at once."
  [ctx {:keys [filename table columns] :as data-spec}]
  (if-let [file-to-load (find-input-file ctx filename)]
    (do
      (log/info "Loading" filename)
      (with-open [in-file (util/bom-safe-reader file-to-load :encoding "UTF-8")]
        (let [headers (->> in-file
                           .readLine
                           csv/read-csv
                           first
                           (map #(clojure.string/replace % #"\W" "")))
              headers-count (count headers)
              sql-table (get-in ctx [:tables table])
              column-names (map :name columns)
              required-header-names (->> columns
                                         (filter :required)
                                         (map :name))
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
                                    (db.util/retry-chunk-without-dupe-ids ctx sql-table chunk-values)
                                    (assoc-in ctx [:fatal (:name sql-table) @line-number :unknown-sql-error]
                                              [message]))))))
                          ctx))
                      ctx (db.util/chunk-rows (csv/read-csv in-file)
                                              sqlite/statement-parameter-limit)))))))
    ctx))

(defn-traced load-csvs [ctx]
  (reduce bulk-import-and-validate-csv ctx (:data-specs ctx)))

(defn error-on-missing-files
  "Add errors for any file with the :required key in the data-spec."
  [{:keys [data-specs] :as ctx}]
  (let [required-files (filter :required data-specs)]
    (reduce (fn [ctx {:keys [filename required table]}]
              (if (find-input-file ctx filename)
                ctx
                (assoc-in ctx
                          [required table :global :missing-csv]
                          [(str filename " is missing")])))
            ctx required-files)))
