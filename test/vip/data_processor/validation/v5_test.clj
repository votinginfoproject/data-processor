(ns vip.data-processor.validation.v5-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.db.translations.transformer :as transformer]
             [vip.data-processor.errors.process :as process]
             [vip.data-processor.validation.csv :as csv]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5 :as v5]
             [clojure.java.io :as io]
             [korma.core :as korma]
             [clojure.core.async :as a])
  (:import [java.nio.file Files]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres full-good-v5-test
  (let [errors-chan (a/chan 100)
        ctx {:errors-chan errors-chan
             :xml-source-file-path (xml-input "v5_sample_feed.xml")
             :spec-version nil
             :spec-family nil
             :pipeline (concat [psql/start-run
                                xml/determine-spec-version
                                xml/load-xml-ltree]
                               v5/validations)}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems errors {})))

(deftest ^:postgres full-good-v5-csv-test
  (let [csvs (-> "csv/5-2/full-good-run"
                 io/resource
                 io/as-file
                 .toPath
                 Files/list
                 .iterator
                 iterator-seq)
        errors-chan (a/chan 100)
        ctx {:csv-source-file-paths csvs
             :errors-chan errors-chan
             :spec-version nil
             :spec-family nil
             :pipeline (concat [psql/start-run
                                csv/determine-spec-version
                                psql/prep-v5-2-run
                                process/process-v5-validations
                                csv/load-csvs]
                               transformer/transformers
                               v5/validations
                               [psql/store-spec-version
                                psql/store-public-id])}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems errors {})

    (testing "the election state, type, and date are saved to the results table"
      (let [results-values (-> (korma/select psql/results
                                (korma/fields :id :start_time :public_id :state :election_type :election_date :vip_id)
                                (korma/where {:id (:import-id out-ctx)}))
                            first)]
        (is (= (:election_date results-values) "10/08/2016"))
        (is (= (:election_type results-values) "Edible"))
        (is (= (:state results-values) "Virginia"))
        (is (= (:vip_id results-values) "51"))))

    (testing "the data for building a public_id is correctly fetched"
      (is (= {:date "10/08/2016"
              :election-type "Edible"
              :state "Virginia"
              :vip-id "51"}
             (dissoc
              (psql/get-xml-tree-public-id-data out-ctx)
              :import-id))))))
