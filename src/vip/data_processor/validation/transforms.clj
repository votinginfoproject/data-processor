(ns vip.data-processor.validation.transforms
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.db :as db]))

(defn read-edn-sqs-message [ctx]
  (assoc ctx :input (edn/read-string (get-in ctx [:input :body]))))

(defn assert-filename [ctx]
  (if-let [filename (get-in ctx [:input :filename])]
    (assoc ctx :filename filename)
    (assoc ctx :stop "No filename!")))

(defn attach-sqlite-db [ctx]
  (merge ctx (sqlite/temp-db (:filename ctx))))

(defn download-from-s3 [ctx]
  (let [filename (get-in ctx [:input :filename])
        file (s3/download filename)]
    (assoc ctx :input file)))

(def xml-validations
  [(fn [ctx] (assoc ctx :stop "This is an XML feed"))])

(def csv-validations
  [(csv/add-csv-specs csv/csv-specs)
   csv/remove-bad-filenames
   (csv/error-on-missing-file "election.txt")
   (csv/error-on-missing-file "source.txt")
   (csv-files/validate-dependencies csv-files/file-dependencies)
   csv/load-csvs
   db/validate-no-duplicated-ids
   db/validate-no-duplicated-rows
   db/validate-references
   db/validate-jurisdiction-references
   db/validate-no-unreferenced-rows
   db/validate-no-overlapping-street-segments])

(defn xml-csv-branch [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last))
                             set)
        filetype-validations (condp set/superset? file-extensions
                               #{"txt" "csv"} csv-validations
                               #{"xml"} xml-validations)]
    (update ctx :pipeline (partial concat filetype-validations))))
