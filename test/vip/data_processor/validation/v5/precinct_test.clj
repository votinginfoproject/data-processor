(ns vip.data-processor.validation.v5.precinct-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.precinct :as precinct]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-precincts.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        precinct/validate-no-missing-names]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing Names are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :precincts
                            :identifier "VipObject.0.Precinct.0.Name"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :precincts
                            :identifier "VipObject.0.Precinct.1.Name"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :precincts
                             :identifier "VipObject.0.Precinct.2.Name"
                             :error-type :missing})
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :precincts
                             :identifier "VipObject.0.Precinct.3.Name"
                             :error-type :missing}))))

(deftest ^:postgres validate-no-missing-locality-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-precincts.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        precinct/validate-no-missing-locality-ids]
             :errors-chan errors-chan}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing LocalityIds are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :precincts
                            :identifier "VipObject.0.Precinct.0.LocalityId"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :precincts
                            :identifier "VipObject.0.Precinct.2.LocalityId"
                            :error-type :missing})))
    (testing "doesn't for those that aren't"
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :precincts
                             :identifier "VipObject.0.Precinct.1.LocalityId"
                             :error-type :missing})
      (assert-no-problems-2 errors
                            {:severity :errors
                             :scope :precincts
                             :identifier "VipObject.0.Precinct.3.LocalityId"
                             :error-type :missing}))))
