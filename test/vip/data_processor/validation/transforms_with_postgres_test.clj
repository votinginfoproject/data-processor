(ns vip.data-processor.validation.transforms-with-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.csv.file-set :as csv-files]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.db.sqlite :as sqlite]
            [squishy.data-readers]
            [korma.core :as korma]
            [clojure.core.async :as a]
            [clojure.java.io :as io]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres csv-validations-test
  (testing "full run on good files"
    (let [db (sqlite/temp-db "good-run-test" "3.0")
          file-paths (->> v3-0/data-specs
                          (map
                           #(io/as-file (io/resource (str "csv/full-good-run/" (:filename %)))))
                          (remove nil?)
                          (map #(.toPath %)))
          errors-chan (a/chan 100)
          ctx (merge {:test-name "baaaaaaaar"
                      :csv-source-file-paths file-paths
                      :errors-chan errors-chan
                      :spec-version nil
                      :spec-family nil
                      :pipeline (concat
                                 [psql/start-run
                                  (data-spec/add-data-specs v3-0/data-specs)
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
