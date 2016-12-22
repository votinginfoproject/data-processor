(ns vip.data-processor.validation.v5.locality-test
  (:require [vip.data-processor.validation.v5.locality :as v5.locality]
            [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor :as data-processor]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.v5 :as v5]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-names)
        errors (all-errors errors-chan)]
    (testing "name missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.Name"
                            :error-type :missing})))
    (testing "name present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.Name"
                           :error-type :missing}))))

(deftest ^:postgres validate-no-missing-state-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-state-ids)
        errors (all-errors errors-chan)]
    (testing "state-id missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.StateId"
                            :error-type :missing})))
    (testing "state-id present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.StateId"
                           :error-type :missing}))))

(deftest ^:postgres locality-summary-test
  (let [errors-chan (a/chan 100)
        pipeline [psql/start-run
                        t/xml-csv-branch
                        data-processor/add-validations
                        errors/close-errors-chan
                        errors/await-statistics]
        ctx {:input (xml-input "v5-locality-summaries.xml")
             :spec-version (atom "5.1.2")
             :errors-chan errors-chan
             :pipeline pipeline}

        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)
        results-id (:import-id out-ctx)]

    (testing "we have three localities"
      (let [localities (-> (korma/select psql/v5-dashboard-localities
                             (korma/aggregate (count :*) :count)
                             (korma/where {:results_id results-id}))
                           first)]
        (is (= 3 (:count localities)))))

    (testing "a locality element can have a number of errors"
      (let [salida (-> (korma/select psql/v5-dashboard-localities
                         (korma/where {:id "loc3" :results_id results-id}))
                       first)]
        (is (= (:type salida) "city"))
        (is (= (:street_segment_errors salida) 1))
        (is (= (:voter_service_errors salida) 1))
        (is (= (:hours_open_errors salida) 2))
        (is (= (:department_errors salida) 0))
        (is (= (:election_administration_errors salida) 0))
        (is (= (:polling_location_errors salida) 0))))))
