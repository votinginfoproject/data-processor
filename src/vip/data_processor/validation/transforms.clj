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
  (let [db (sqlite/temp-db (:import-id ctx))
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
  [xml/load-xml])

(def csv-validations
  [csv/remove-bad-filenames
   (csv/error-on-missing-file "election.txt")
   (csv/error-on-missing-file "source.txt")
   (csv-files/validate-dependencies csv-files/file-dependencies)
   csv/load-csvs])

(defn remove-invalid-extensions [ctx]
  (let [files (:input ctx)
        valid-extensions #{"csv" "txt" "xml"}
        invalid-fn (fn [file] 
                     (not (some valid-extensions
                                (-> file .getName 
                                    (s/split #"\.")))))
        {valid-files false invalid-files true} (group-by invalid-fn files)]
    (-> ctx 
        (assoc :input valid-files)
        (assoc-in [:warnings :import :global :invalid-extensions] 
                  (map #(.getName %) invalid-files)))))

(defn xml-csv-branch [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last))
                             set)
        filetype-validations (condp set/superset? file-extensions
                               #{"txt" "csv"} csv-validations
                               #{"xml"} xml-validations)]
    (update ctx :pipeline (partial concat filetype-validations))))
