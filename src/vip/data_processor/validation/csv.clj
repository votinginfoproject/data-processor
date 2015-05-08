(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
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
      (-> ctx
          (assoc-in [:warnings :validate-filenames :bad-filenames]
                    (apply str "Bad filenames: "
                           (interpose ", " (->> bad-files (map file-name) sort))))
          (assoc :input good-files))
      ctx)))

(defn read-csv-with-headers [file-handle]
  (let [raw-rows (csv/read-csv file-handle)
        headers (first raw-rows)
        rows (rest raw-rows)]
    {:headers headers
     :contents (map (partial zipmap headers) rows)
     :bad-rows (remove (fn [[_ row]] (= (count headers) (count row)))
                       (map list (iterate inc 2) rows))}))

(defn report-bad-rows [ctx filename expected-number bad-rows]
  (if-not (empty? bad-rows)
    (reduce (fn [ctx [line-number row]]
              (assoc-in ctx [:critical filename :number-of-values
                             (keyword (str "line-" line-number))]
                        (str "Expected " expected-number
                             " values, found " (count row))))
            ctx bad-rows)
    ctx))

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (.getName %)))
       first))

(defn validate-format-rules [ctx rows {:keys [filename columns]}]
  (let [format-rules (data-spec/create-format-rules filename columns)
        line-number (atom 1)]
    (reduce (fn [ctx row]
              (data-spec/apply-format-rules format-rules ctx row (swap! line-number inc)))
            ctx rows)))

(defn load-csv [ctx {:keys [filename table columns] :as data-spec}]
  (if-let [file-to-load (find-input-file ctx filename)]
    (with-open [in-file (io/reader file-to-load :encoding "UTF-8")]
      (let [sql-table (get-in ctx [:tables table])
            column-names (map :name columns)
            required-header-names (->> columns (filter :required) (map :name))
            {:keys [headers contents bad-rows]} (read-csv-with-headers in-file)
            extraneous-headers (seq (set/difference (set headers) (set column-names)))
            ctx (if extraneous-headers
                  (assoc-in ctx [:warnings table :extraneous-headers]
                            (str/join ", " extraneous-headers))
                  ctx)
            ctx (report-bad-rows ctx table (count headers) bad-rows)
            contents (map #(select-keys % column-names) contents)]
        (if (empty? (set/intersection (set headers) (set column-names)))
          (assoc-in ctx [:critical table :headers] "No header row")
          (if-let [missing-headers (seq (set/difference (set required-header-names) (set headers)))]
            (assoc-in ctx [:critical table :headers]
                      (str "Missing headers: " (str/join ", " missing-headers)))
            (let [ctx (validate-format-rules ctx contents data-spec)
                  transforms (apply comp (data-spec/translation-fns columns))
                  transformed-contents (map transforms contents)]
              (sqlite/bulk-import transformed-contents sql-table)
              ctx)))))
    ctx))

(defn load-csvs [ctx]
  (reduce load-csv ctx (:data-specs ctx)))

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
          (assoc-in ctx [report-type table :missing-csv] (str filename " is missing")))))))

(def ^{:doc "Generates a validation function that adds a warning when
  the given filename is missing from the input"}
  warn-on-missing-file
  (add-report-on-missing-file-fn :warnings))

(def ^{:doc "Generates a validation function that adds an error when
  the given filename is missing from the input"}
  error-on-missing-file
  (add-report-on-missing-file-fn :errors))
