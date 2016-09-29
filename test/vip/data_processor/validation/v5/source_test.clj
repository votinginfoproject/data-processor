(ns vip.data-processor.validation.v5.source-test
  (:require [vip.data-processor.validation.v5.source :as v5.source]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-one-source-test
  (testing "more than one Source element is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-more-than-one-source.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-one-source]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :source
                            :identifier "VipObject.0.Source"
                            :error-type :count}))))
  (testing "one and only one Source element is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-one-source.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-one-source]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-name-test
  (testing "missing Name is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-without-name.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :source
                            :identifier "VipObject.0.Source.0.Name"
                            :error-type :missing}))))
  (testing "Name present is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-with-name.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})))
  (testing "Name present is OK even if it's not first"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-second-with-name-second.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-name]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-date-time-test
  (testing "missing DateTime is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-without-date-time.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :source
                            :identifier "VipObject.0.Source.0.DateTime"
                            :error-type :missing}))))
  (testing "DateTime present is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-with-date-time.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})))
  (testing "DateTime present is OK even if it's not first"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-second-with-date-time-second.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-date-time]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-vip-id-test
  (testing "missing VipId is a fatal error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-without-vip-id.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :source
                            :identifier "VipObject.0.Source.0.VipId"
                            :error-type :missing}))))
  (testing "VipId present is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-with-vip-id.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))

(deftest ^:postgres validate-vip-id-valid-fips-test
  (testing "invalid 2-digit FIPS in VipId is a critical error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-vip-id-invalid-2-digit-fips.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id-valid-fips]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :critical
                            :scope :source
                            :identifier "VipObject.0.Source.1.VipId.2"
                            :error-type :invalid-fips}))))
  (testing "valid 2-digit FIPS in VipId is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-vip-id-valid-2-digit-fips.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id-valid-fips]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})))
  (testing "invalid 5-digit FIPS in VipId is a critical error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-vip-id-invalid-5-digit-fips.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id-valid-fips]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :critical
                            :scope :source
                            :identifier "VipObject.0.Source.1.VipId.2"
                            :error-type :invalid-fips}))))
  (testing "valid 5-digit FIPS in VipId is OK"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-source-vip-id-valid-5-digit-fips.xml")
               :pipeline [psql/start-run
                          xml/load-xml-ltree
                          v5.source/validate-vip-id-valid-fips]
               :errors-chan errors-chan}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {}))))
