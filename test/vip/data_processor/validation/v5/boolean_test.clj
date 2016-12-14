(ns vip.data-processor.validation.v5.boolean-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :refer :all]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.boolean :as v5.boolean]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres boolean-isstatewide-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-isstatewide-incorrect-boolean.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Election.0.IsStatewide.5",
                            :error-type :format
                            :error-value "True"})))))

(deftest ^:postgres boolean-ispartisan-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-ispartisan-incorrect-boolean.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Office.0.IsPartisan.2",
                            :error-type :format
                            :error-value "False"})))))

(deftest ^:postgres boolean-isdropbox-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-isdropbox-incorrect-boolean.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.PollingLocation.0.IsDropBox.4",
                            :error-type :format
                            :error-value "True"})))))

(deftest ^:postgres boolean-ismailonly-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-ismailonly-incorrect-boolean.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Precinct.0.IsMailOnly.4",
                            :error-type :format
                            :error-value "True"})))))

(deftest ^:postgres boolean-haselectiondayregistration-incorrect-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-haselectiondayregistration-incorrect-boolean.xml")
             :pipeline [psql/start-run
                        load-xml-ltree
                        v5.boolean/validate-booleans]
              :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "catch True instead of true"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :boolean
                            :identifier "VipObject.0.Election.0.IsStatewide.5",
                            :error-type :format
                            :error-value "True"})))))
