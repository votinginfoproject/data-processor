(ns vip.data-processor.validation.fips-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.fips :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.db.sqlite :as sqlite]
            [vip.data-processor.pipeline :as pipeline]))

(deftest validate-valid-source-vip-id-test
  (testing "adds an error if the source's vip_id is bad from a csv"
    (let [ctx (merge {:input (csv-inputs ["invalid-source-vip-id/source.txt"])
                      :pipeline [(data-spec/add-data-specs data-spec/data-specs)
                                 csv/load-csvs
                                 validate-valid-source-vip-id]}
                     (sqlite/temp-db "invalid-source-vip-id-csv"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (get-in out-ctx [:errors :source :invalid-vip-id]))))
  (testing "adds an error if the source's vip_id is bad from a xml"
    (let [ctx (merge {:input (xml-input "invalid-source-vip-id.xml")
                      :data-specs data-spec/data-specs
                      :pipeline [xml/load-xml validate-valid-source-vip-id]}
                     (sqlite/temp-db "invalid-source-vip-id-xml"))
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= "99999" (get-in out-ctx [:errors :source :invalid-vip-id]))))))
