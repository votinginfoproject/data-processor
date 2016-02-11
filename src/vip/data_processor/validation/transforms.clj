(ns vip.data-processor.validation.transforms
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.xml :as xml]))

(defn read-edn-sqs-message [ctx]
  (assoc ctx :input (edn/read-string (get-in ctx [:input :body]))))

(defn assert-filename [ctx]
  (if-let [filename (get-in ctx [:input :filename])]
    (assoc ctx :filename filename)
    (assoc ctx :stop "No filename!")))

(defn attach-sqlite-db [ctx]
  (let [db (sqlite/temp-db (:import-id ctx) "3.0") ; TODO: set version according to import version
        db-file (get-in db [:db :db])]
    (-> ctx
        (merge db)
        (update :to-be-cleaned conj db-file))))

(defn download-from-s3 [ctx]
  (let [filename (get-in ctx [:input :filename])
        file (s3/download filename)]
    (-> ctx
        (update :to-be-cleaned conj file)
        (assoc :input file))))

(def xml-validations
  [xml/determine-spec-version
   xml/branch-on-spec-version])

(def csv-validations
  [csv/remove-bad-filenames
   csv/error-on-missing-files
   csv/determine-spec-version
   (csv-files/validate-dependencies csv-files/v3-0-file-dependencies) ; TODO: validate file depenencies based import version
   csv/branch-on-spec-version])

(defn remove-invalid-extensions [ctx]
  (let [files (:input ctx)
        valid-extensions #{"csv" "txt" "xml"}
        invalid-fn (fn [file]
                     (not (get valid-extensions
                               (-> file
                                   .getName
                                   (s/split #"\.")
                                   last
                                   s/lower-case))))
        {valid-files false invalid-files true} (group-by invalid-fn files)]
    (-> ctx
        (assoc :input valid-files)
        (assoc-in [:warnings :import :global :invalid-extensions]
                  (map #(.getName %) invalid-files)))))

(defn xml-csv-branch [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last s/lower-case))
                             set)
        filetype-validations (condp set/superset? file-extensions
                               #{"txt" "csv"} csv-validations
                               #{"xml"} xml-validations)]
    (update ctx :pipeline (partial concat filetype-validations))))
