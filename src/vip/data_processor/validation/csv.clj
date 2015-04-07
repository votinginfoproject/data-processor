(ns vip.data-processor.validation.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [vip.data-processor.validation.data-spec :refer [data-specs]]
            [vip.data-processor.db.sqlite :as sqlite]))

(def csv-filenames (set (map :filename data-specs)))

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
          (assoc-in [:warnings :validate-filenames]
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
              (assoc-in ctx [:critical filename line-number "Number of values"]
                        (str "Expected " expected-number " values, found " (count row))))
            ctx bad-rows)
    ctx))

(defn find-input-file [ctx filename]
  (->> ctx
       :input
       (filter #(= filename (.getName %)))
       first))

(defn invalid-utf-8? [string]
  (.contains string "�"))

(defn create-format-rule
  "Create a function that applies a format check for a specific row of
  a CSV import."
  [filename {:keys [name required format]}]
  (let [{:keys [check message]} format
        test-fn (cond
                 (sequential? check) (fn [val] (some #{val} check))
                 (instance? clojure.lang.IFn check) check
                 (instance? java.util.regex.Pattern check) (fn [val] (re-find check val))
                 :else (constantly true))]
    (fn [ctx row line-number]
      (let [val (row name)]
        (cond
          (empty? val)
          (if required
            (assoc-in ctx [:fatal filename line-number name] (str "Missing required column: " name))
            ctx)

          (invalid-utf-8? val)
          (assoc-in ctx [:errors filename line-number name] "Is not valid UTF-8.")

          (not (test-fn val))
          (assoc-in ctx [:errors filename line-number name] message)

          :else ctx)))))

(defn create-format-rules [{:keys [filename columns]}]
  (map (partial create-format-rule filename) columns))

(defn apply-format-rules [rules ctx row line-number]
  (reduce (fn [ctx rule] (rule ctx row line-number)) ctx rules))

(defn validate-format-rules [ctx rows data-spec]
  (let [format-rules (create-format-rules data-spec)
        line-number (atom 1)]
    (reduce (fn [ctx row]
              (apply-format-rules format-rules ctx row (swap! line-number inc)))
            ctx rows)))

(defn create-translation-fn [{:keys [name translate]}]
  (fn [row]
    (if-let [cell (row name)]
      (assoc row name (translate cell))
      row)))

(defn translation-fns [columns]
  (->> columns
       (filter :translate)
       (map create-translation-fn)))

(defn load-csv [ctx {:keys [filename table columns] :as data-spec}]
  (if-let [file-to-load (find-input-file ctx filename)]
    (with-open [in-file (io/reader file-to-load :encoding "UTF-8")]
      (let [sql-table (get-in ctx [:tables table])
            column-names (map :name columns)
            required-header-names (->> columns (filter :required) (map :name))
            {:keys [headers contents bad-rows]} (read-csv-with-headers in-file)
            extraneous-headers (seq (set/difference (set headers) (set column-names)))
            ctx (if extraneous-headers
                  (assoc-in ctx [:warnings filename :extraneous-headers]
                            (str/join ", " extraneous-headers))
                  ctx)
            ctx (report-bad-rows ctx filename (count headers) bad-rows)
            contents (map #(select-keys % column-names) contents)]
        (if (empty? (set/intersection (set headers) (set column-names)))
          (assoc-in ctx [:critical filename :headers] "No header row")
          (if-let [missing-headers (seq (set/difference (set required-header-names) (set headers)))]
            (assoc-in ctx [:critical filename :headers]
                      (str "Missing headers: " (str/join ", " missing-headers)))
            (let [ctx (validate-format-rules ctx contents data-spec)
                  transforms (apply comp (translation-fns columns))
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
    (fn [ctx]
      (if (find-input-file ctx filename)
        ctx
        (assoc-in ctx [report-type filename] (str filename " is missing"))))))

(def ^{:doc "Generates a validation function that adds a warning when
  the given filename is missing from the input"}
  warn-on-missing-file
  (add-report-on-missing-file-fn :warnings))

(def ^{:doc "Generates a validation function that adds an error when
  the given filename is missing from the input"}
  error-on-missing-file
  (add-report-on-missing-file-fn :errors))
