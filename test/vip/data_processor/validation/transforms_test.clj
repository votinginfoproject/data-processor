(ns vip.data-processor.validation.transforms-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.transforms :refer :all]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]
            [korma.core :as korma])
  (:import [java.io File]))

(deftest csv-validations-test
  (testing "full run on good files"
    (let [db (sqlite/temp-db "good-run-test")
          filenames (->> csv/csv-filenames
                         (map #(io/as-file (io/resource (str "full-good-run/" %))))
                         (remove nil?))
          ctx (merge {:input filenames :pipeline csv-validations} db)
          results-ctx (pipeline/run-pipeline ctx)]
      (is (not (contains? results-ctx :stop )))
      (is (not (contains? results-ctx :exception)))
      (assert-no-problems results-ctx []))))
