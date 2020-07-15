(ns vip.data-processor.validation.transforms-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.transforms :refer :all]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]
            [korma.core :as korma]
            [clojure.core.async :as a])
  (:import [java.nio.file Paths]))

(deftest assert-filename-and-bucket-test
  (testing "missing filename"
    (let [in-ctx {:bucket "foo"}
          out-ctx (assert-filename-and-bucket in-ctx)]
      (is (= (:stop out-ctx) "No filename or bucket!"))))
  (testing "missing filename"
    (let [in-ctx {:bucket "foo"}
          out-ctx (assert-filename-and-bucket in-ctx)]
      (is (= (:stop out-ctx) "No filename or bucket!"))))
  (testing "missing bucket"
    (let [in-ctx {:filename "file.zip"}
          out-ctx (assert-filename-and-bucket in-ctx)]
      (is (= (:stop out-ctx) "No filename or bucket!"))))
  (testing "empty bucket"
    (let [in-ctx {:filename "file.zip" :bucket ""}
          out-ctx (assert-filename-and-bucket in-ctx)]
      (is (= (:stop out-ctx) "No filename or bucket!"))))
  (testing "good message"
    (let [in-ctx {:filename "file.zip" :bucket "foo"}
          out-ctx (assert-filename-and-bucket in-ctx)]
      (is (= out-ctx in-ctx)))))

(deftest csv-validations-test
  (testing "full run on good files"
    (let [db (sqlite/temp-db "good-run-test" "3.0")
          file-paths (->> v3-0/data-specs
                          (map
                           #(io/as-file (io/resource (str "csv/full-good-run/" (:filename %)))))
                          (remove nil?)
                          (map #(.toPath %)))
          errors-chan (a/chan 100)
          ctx (merge {:csv-source-file-paths file-paths
                      :errors-chan errors-chan
                      :spec-version nil
                      :spec-family nil
                      :pipeline (concat
                                 [(data-spec/add-data-specs v3-0/data-specs)
                                  csv/error-on-missing-files
                                  csv/determine-spec-version
                                  csv/remove-bad-filenames
                                  sqlite/attach-sqlite-db
                                  process/process-v3-validations
                                  (csv-files/validate-dependencies csv-files/v3-0-file-dependencies)
                                  csv/load-csvs]
                                 db/validations)} db)
          results-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (nil? (:stop results-ctx)))
      (is (nil? (:exception results-ctx)))
      (assert-no-problems errors {}))))

(deftest remove-invalid-extensions-test
  (testing "removes non-csv, txt, or xml files from :extracted-file-paths, placing the good ones in :valid-file-paths"
    (let [errors-chan (a/chan 100)
          ctx {:extracted-file-paths
               [(Paths/get "good-file.xml" (into-array String []))
                (Paths/get "not-so-good-file.xls" (into-array String []))
                (Paths/get "logo.ai" (into-array String []))]
               :errors-chan errors-chan}
          results-ctx (remove-invalid-extensions ctx)
          errors (all-errors errors-chan)]
      (is (= 1 (count (:valid-file-paths results-ctx))))
      (is (= "good-file.xml" (-> results-ctx :valid-file-paths first .toFile .getName)))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :import
                            :identifier :global
                            :error-type :invalid-extensions
                            :error-value '("not-so-good-file.xls" "logo.ai")}))))
  (testing "allows uppercase file extensions"
    (let [errors-chan (a/chan 100)
          ctx {:extracted-file-paths
               [(Paths/get "this-is-okay.XML" (into-array String []))
                (Paths/get "so-is-this.TXT" (into-array String []))
                (Paths/get "but-not-this.NAIL" (into-array String []))]
               :errors-chan errors-chan}
          results-ctx (remove-invalid-extensions ctx)
          errors (all-errors errors-chan)]
      (is (= [(Paths/get "this-is-okay.XML" (into-array String []))
              (Paths/get "so-is-this.TXT" (into-array String []))]
             (:valid-file-paths results-ctx)))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :import
                            :identifier :global
                            :error-type :invalid-extensions
                            :error-value '("but-not-this.NAIL")})))))
