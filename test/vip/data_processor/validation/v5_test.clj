(ns vip.data-processor.validation.v5-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.csv :as csv]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5 :as v5]
             [clojure.java.io :as io]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres full-good-v5-test
  (let [ctx {:input (xml-input "v5_sample_feed.xml")
             :pipeline (concat [psql/start-run
                                xml/determine-spec-version
                                xml/load-xml-ltree]
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)]
    (assert-no-problems out-ctx [])))

(deftest ^:postgres full-good-v51-csv-test
  (let [csvs (-> "csv/5-1/full-good-run"
                 io/resource
                 io/as-file
                 .listFiles
                 seq)
        ctx {:input csvs
             :pipeline (concat [psql/start-run
                                csv/determine-spec-version]
                               (csv/version-pipelines "5.1")
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)]
    (assert-no-problems out-ctx [])

    (testing "the data for building a public_id is correctly fetched"
      (is (= {:date "10/08/2016"
              :election-type "Edible"
              :state "Virginia"}
             (dissoc
              (psql/get-xml-tree-public-id-data out-ctx)
              :import-id))))))
