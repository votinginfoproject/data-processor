(ns vip.data-processor.validation.transforms
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [vip.data-processor.s3 :as s3]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.errors :as errors]))

(defn read-edn-sqs-message [ctx]
  (assoc ctx :input (edn/read-string (get-in ctx [:input :body]))))

(defn assert-filename [ctx]
  (if-let [filename (get-in ctx [:input :filename])]
    (assoc ctx :filename filename)
    (assoc ctx :stop "No filename!")))

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
  [csv/error-on-missing-files
   csv/determine-spec-version
   csv/remove-bad-filenames
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
        {valid-files false invalid-files true} (group-by invalid-fn files)
        ctx (assoc ctx :input valid-files)]
    (if (seq invalid-files)
      (errors/add-errors ctx :warnings :import :global :invalid-extensions
                         (map #(.getName %) invalid-files))
      ctx)))

(defn xml-csv-branch [ctx]
  (let [file-extensions (->> ctx
                             :input
                             (map #(-> % str (s/split #"\.") last s/lower-case))
                             set)
        filetype-validations (condp set/superset? file-extensions
                               #{"txt" "csv"} csv-validations
                               #{"xml"} xml-validations)]
    (update ctx :pipeline (partial concat filetype-validations))))
