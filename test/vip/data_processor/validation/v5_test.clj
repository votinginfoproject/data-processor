(ns vip.data-processor.validation.v5-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.csv :as csv]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5 :as v5]
             [clojure.java.io :as io]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres full-good-v5-test
  (let [errors-chan (a/chan 100)
        ctx {:errors-chan errors-chan
             :input (xml-input "v5_sample_feed.xml")
             :spec-version (atom nil)
             :pipeline (concat [psql/start-run
                                xml/determine-spec-version
                                xml/load-xml-ltree]
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems-2 errors {})))

(deftest ^:postgres full-good-v51-csv-test
  (let [csvs (-> "csv/5-1/full-good-run"
                 io/resource
                 io/as-file
                 .listFiles
                 seq)
        errors-chan (a/chan 100)
        ctx {:input csvs
             :errors-chan errors-chan
             :spec-version (atom nil)
             :pipeline (concat [psql/start-run
                                csv/determine-spec-version]
                               (csv/version-pipelines "5.1")
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems-2 errors {})

    (testing "the data for building a public_id is correctly fetched"
      (is (= {:date "10/08/2016"
              :election-type "Edible"
              :state "Virginia"}
             (dissoc
              (psql/get-xml-tree-public-id-data out-ctx)
              :import-id))))))
