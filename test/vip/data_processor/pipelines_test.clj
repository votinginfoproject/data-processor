(ns vip.data-processor.pipelines-test
  (:require [vip.data-processor.pipelines :refer :all]
            [vip.data-processor.pipelines.csv.v3 :as csv-v3]
            [vip.data-processor.pipelines.csv.v5 :as csv-v5]
            [vip.data-processor.pipelines.xml.v3 :as xml-v3]
            [vip.data-processor.pipelines.xml.v5 :as xml-v5]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]))

(deftest choose-pipeline-test
  (testing "3.0 CSV ctx"
    (let [ctx {:format :csv
               :spec-family "3.0"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= (:pipeline ctx-prime)
             csv-v3/pipeline))))
  (testing "5.2 CSV ctx"
    (let [ctx {:format :csv
               :spec-family "5.2"
               :spec-version "5.1.2"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= (:pipeline ctx-prime)
             csv-v5/pipeline))))
  (testing "Unexpected 5.X version CSV ctx"
    (let [ctx {:format :csv
               :spec-family "5.3"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= "Unsupported CSV version: 5.3"
             (:stop ctx-prime)))))
  (testing "3.0 XML ctx"
    (let [ctx {:format :xml
               :spec-family "3.0"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= (:pipeline ctx-prime)
             xml-v3/pipeline))))
  (testing "5.2 XML ctx"
    (let [ctx {:format :xml
               :spec-family "5.2"
               :spec-version "5.1.2"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= (:pipeline ctx-prime)
             xml-v5/pipeline))))
  (testing "Unexpected 5.X version XML ctx"
    (let [ctx {:format :xml
               :spec-family "5.3"
               :pipeline []}
          ctx-prime (choose-pipeline ctx)]
      (is (= "Unsupported XML version: 5.3"))))
  (testing "Unknown format"
    (let [ctx {:format :pdf
               :spec-family "5.2"
               :pipeline []}]
      (try
        (choose-pipeline ctx)
        (is (= 0 1)) ;;test shouldn't reach this point
        (catch Exception ex
          (is (= (.getMessage ex)
                 "No pipeline matching format and spec-family version")))))))
