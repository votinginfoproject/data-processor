(ns vip.data-processor.validation.db.v3-0.fips-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.db.v3-0.fips :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]
            [clojure.core.async :as a]))

(deftest validate-valid-source-vip-id-test
  (testing "adds an error if the source's vip_id is bad from a csv"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (csv-inputs ["invalid-source-vip-id/source.txt"])
                      :errors-chan errors-chan
                      :pipeline [(data-spec/add-data-specs v3-0/data-specs)
                                 csv/load-csvs
                                 validate-valid-source-vip-id]}
                     (sqlite/temp-db "invalid-source-vip-id-csv" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :errors
                                   :scope :sources
                                   :identifier 1
                                   :error-type :invalid-vip-id
                                   :error-value "5199955554447"}))))
  (testing "adds an error if the source's vip_id is bad from a xml"
    (let [errors-chan (a/chan 100)
          ctx (merge {:input (xml-input "invalid-source-vip-id.xml")
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :pipeline [xml/load-xml validate-valid-source-vip-id]}
                     (sqlite/temp-db "invalid-source-vip-id-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors {:severity :errors
                                   :scope :sources
                                   :identifier 0
                                   :error-type :invalid-vip-id
                                   :error-value "99999"})))))
